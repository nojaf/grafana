package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	es "github.com/grafana/grafana/pkg/tsdb/elasticsearch/client"
)

// Ensure Service implements the StreamHandler interface
var _ backend.StreamHandler = (*Service)(nil)

const (
	// headerFromExpression is used by data sources to identify expression queries
	headerFromExpression = "X-Grafana-From-Expr"
	// headerFromAlert is used by data sources to identify alert queries
	headerFromAlert = "FromAlert"
	// this is the default value for the maxConcurrentShardRequests setting - it should be in sync with the default value in the datasource config settings
	defaultMaxConcurrentShardRequests = int64(5)
	// Path for log streaming
	logsStreamPath = "logs"
	// Default polling interval for live tailing
	defaultPollInterval = 1 * time.Second
	// Default number of logs to fetch per poll
	defaultStreamBatchSize = 1000
)

// logStream holds state for an active live tailing stream
type logStream struct {
	ctx           context.Context
	cancel        context.CancelFunc
	dsInfo        *es.DatasourceInfo
	queryJSON     json.RawMessage // Store the raw query JSON received from frontend
	lastTimestamp []interface{}   // Use interface{} for search_after
	mu            sync.Mutex      // Protects lastTimestamp
}

type Service struct {
	im      instancemgmt.InstanceManager
	logger  log.Logger
	streams sync.Map // Use sync.Map for concurrent-safe map operations
}

func ProvideService(httpClientProvider *httpclient.Provider) *Service {
	backend.Logger.Warn("!!! Elasticsearch Service Provider INITIALIZED !!!") // Use Warn to make it stand out
	return &Service{
		im:      datasource.NewInstanceManager(newInstanceSettings(httpClientProvider)),
		logger:  backend.NewLoggerWith("logger", "tsdb.elasticsearch"),
		streams: sync.Map{},
	}
}

func (s *Service) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	dsInfo, err := s.getDSInfo(ctx, req.PluginContext)
	_, fromAlert := req.Headers[headerFromAlert]
	logger := s.logger.FromContext(ctx).With("fromAlert", fromAlert)

	if err != nil {
		logger.Error("Failed to get data source info", "error", err)
		return &backend.QueryDataResponse{}, err
	}

	// QueryData does NOT handle streaming requests.
	return queryData(ctx, req, dsInfo, logger)
}

// separate function to allow testing the whole transformation and query flow
func queryData(ctx context.Context, req *backend.QueryDataRequest, dsInfo *es.DatasourceInfo, logger log.Logger) (*backend.QueryDataResponse, error) {
	if len(req.Queries) == 0 {
		return &backend.QueryDataResponse{}, fmt.Errorf("query contains no queries")
	}

	client, err := es.NewClient(ctx, dsInfo, logger)
	if err != nil {
		return &backend.QueryDataResponse{}, err
	}
	// This is where the main query logic resides (dataquery.go?)
	queryExecutor := newElasticsearchDataQuery(ctx, client, req, logger) // Assuming this function exists
	return queryExecutor.execute()                                       // Assuming this method exists
}

// --- StreamHandler Implementation ---

// SubscribeStream handles the initial subscription request from the frontend.
func (s *Service) SubscribeStream(ctx context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	logger := s.logger.FromContext(ctx)
	streamPath := req.Path

	logger.Info("SubscribeStream invoked",
		"path", streamPath,
		"dataLength", len(req.Data),
		"dataContent", string(req.Data))

	// We only support streaming logs for now via this specific path
	if streamPath != logsStreamPath {
		logger.Warn("Received subscribe request for unsupported path", "path", streamPath)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusNotFound,
		}, nil
	}

	// Check if data is empty
	if len(req.Data) == 0 {
		logger.Error("Empty data received in subscription request")
		return nil, fmt.Errorf("subscription request contains no data")
	}

	// Create a new context for this stream that can be cancelled
	streamCtx, cancelFn := context.WithCancel(ctx)

	// Store stream state with the query data
	streamState := &logStream{
		ctx:       streamCtx,
		cancel:    cancelFn,
		queryJSON: req.Data,
		dsInfo:    nil, // This will be set in RunStream
	}

	s.streams.Store(streamPath, streamState)
	logger.Debug("Stored new stream state", "path", streamPath, "hasQueryData", len(req.Data) > 0)

	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

// RunStream handles the actual data streaming for a subscribed path.
func (s *Service) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	backend.Logger.Warn("!!! RunStream Top Level Entry !!!", "path", req.Path, "reqDataLength", len(req.Data), "reqDataContent", string(req.Data))
	logger := s.logger.FromContext(ctx)
	streamPath := req.Path
	logger.Info("RunStream invoked", "path", streamPath)

	val, ok := s.streams.Load(streamPath)
	if !ok {
		logger.Warn("Received run request for unknown stream", "path", streamPath)
		return fmt.Errorf("stream not found: %s", streamPath)
	}
	stream := val.(*logStream)
	logger.Debug("Loaded stream state for run", "path", streamPath)

	// Clean up the stream state when the run function exits
	defer func() {
		logger.Debug("RunStream exiting, deleting stream state", "path", streamPath)
		s.streams.Delete(streamPath)
		stream.cancel() // Ensure the stream context is cancelled
	}()

	// --- Get Datasource Info and Query Details ---
	dsInfo, err := s.getDSInfo(stream.ctx, req.PluginContext)
	if err != nil {
		logger.Error("Failed to get datasource info for stream", "path", streamPath, "error", err)
		return err
	}
	stream.dsInfo = dsInfo
	logger.Debug("Datasource info loaded for stream", "path", streamPath, "dsId", dsInfo.ID)

	// Store the raw query JSON, validation happens later
	stream.queryJSON = req.Data
	logger.Debug("Received query JSON for stream", "path", streamPath, "queryLength", len(stream.queryJSON))

	// Validate the query *structure* (basic check before loop)
	var baseQuery struct { // Define struct for basic validation
		Metrics []struct {
			Type string `json:"type"`
		} `json:"metrics"`
		Query string `json:"query"`
	}
	if err := json.Unmarshal(stream.queryJSON, &baseQuery); err != nil {
		logger.Error("Failed to unmarshal base query for stream validation", "path", streamPath, "error", err)
		return fmt.Errorf("invalid query format: %w", err)
	}
	if len(baseQuery.Metrics) != 1 || baseQuery.Metrics[0].Type != "logs" {
		logger.Error("Attempted to stream non-log query", "path", streamPath, "query", string(stream.queryJSON))
		return errors.New("live tailing only supports queries with a single 'logs' metric type")
	}
	logger.Debug("Query validated for streaming", "path", streamPath)

	// Initialize lastTimestamp with the current time for the initial search_after
	stream.lastTimestamp = []interface{}{time.Now().UnixMilli()} // Using milliseconds epoch
	logger.Debug("Initialized lastTimestamp", "path", streamPath, "value", stream.lastTimestamp)

	logger.Info("Stream initialized and ready to poll", "path", streamPath, "query", baseQuery.Query, "timeField", stream.dsInfo.ConfiguredFields.TimeField)

	// --- Polling Loop ---
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case tickTime := <-ticker.C:
			logger.Debug("Polling tick received", "path", streamPath, "tickTime", tickTime)
			err := s.fetchAndSendNewLogs(stream, sender, logger)
			if err != nil {
				// Log error but continue loop
				logger.Error("Error during fetchAndSendNewLogs", "path", streamPath, "error", err)
			}
		case <-stream.ctx.Done():
			logger.Info("Stream context cancelled, stopping RunStream", "path", streamPath, "reason", stream.ctx.Err())
			return nil // Normal exit
		case <-ctx.Done():
			logger.Info("RunStream parent context cancelled, stopping RunStream", "path", streamPath, "reason", ctx.Err())
			return nil // Normal exit
		}
	}
}

// fetchAndSendNewLogs queries Elasticsearch for new logs and sends them to the frontend.
func (s *Service) fetchAndSendNewLogs(stream *logStream, sender *backend.StreamSender, logger log.Logger) error {
	logger.Debug("fetchAndSendNewLogs: Attempting to create ES client")
	client, err := es.NewClient(stream.ctx, stream.dsInfo, logger)
	if err != nil {
		return fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}
	logger.Debug("fetchAndSendNewLogs: ES client created")

	// 1. Build the Elasticsearch Query Payload
	stream.mu.Lock()
	currentSearchAfter := stream.lastTimestamp
	stream.mu.Unlock()
	logger.Debug("fetchAndSendNewLogs: Got current searchAfter value", "value", currentSearchAfter)

	esTimeField := stream.dsInfo.ConfiguredFields.TimeField
	if esTimeField == "" {
		logger.Error("fetchAndSendNewLogs: TimeField is not configured")
		return errors.New("timeField is not configured in datasource settings (required for streaming)")
	}

	// We need the Lucene query string from the raw JSON
	var queryDetails struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(stream.queryJSON, &queryDetails); err != nil {
		logger.Error("fetchAndSendNewLogs: Failed to parse query string", "error", err)
		return fmt.Errorf("failed to parse Lucene query string from query JSON: %w", err)
	}

	logger.Debug("fetchAndSendNewLogs: Building query body map", "luceneQuery", queryDetails.Query, "searchAfter", currentSearchAfter, "timeField", esTimeField)
	queryBodyMap, err := buildLiveTailQueryBodyMap(queryDetails.Query, currentSearchAfter, esTimeField)
	if err != nil {
		logger.Error("fetchAndSendNewLogs: Failed to build query body map", "error", err)
		return fmt.Errorf("failed to build live tail query body: %w", err)
	}

	// 2. Execute the Search Request
	searchRequestBody := &es.SearchRequest{
		Size:        defaultStreamBatchSize,
		CustomProps: queryBodyMap,
		Index:       stream.dsInfo.Database,
		Interval:    defaultPollInterval,
	}

	multiSearchReq := &es.MultiSearchRequest{
		Requests: []*es.SearchRequest{searchRequestBody},
	}

	logger.Debug("fetchAndSendNewLogs: Executing live tail search", "index", searchRequestBody.Index, "queryBody", queryBodyMap)
	execStart := time.Now()

	res, err := client.ExecuteMultisearch(multiSearchReq)
	execDuration := time.Since(execStart)
	if err != nil {
		logger.Error("fetchAndSendNewLogs: Live tail search execution failed", "error", err, "duration", execDuration, "queryBody", queryBodyMap)
		return fmt.Errorf("live tail search execution failed: %w", err)
	}
	logger.Debug("fetchAndSendNewLogs: Live tail search successful", "duration", execDuration)

	if len(res.Responses) == 0 {
		logger.Error("fetchAndSendNewLogs: Received empty multisearch response array")
		return errors.New("received empty multisearch response array")
	}
	singleResponse := res.Responses[0]

	if singleResponse.Error != nil {
		errorBytes, _ := json.Marshal(singleResponse.Error)
		logger.Error("fetchAndSendNewLogs: Elasticsearch query error during live tail", "error", string(errorBytes), "queryBody", queryBodyMap)
		return fmt.Errorf("elasticsearch query error: %s", string(errorBytes))
	}

	// 3. Process the Response
	if singleResponse.Hits == nil || len(singleResponse.Hits.Hits) == 0 {
		logger.Debug("fetchAndSendNewLogs: No new logs found in response")
		return nil // No new logs, not an error
	}

	hits := singleResponse.Hits.Hits
	numHits := len(hits)
	logger.Debug("fetchAndSendNewLogs: Received new logs", "count", numHits)

	// --- Create DataFrame ---
	logger.Debug("fetchAndSendNewLogs: Parsing hits to DataFrame")
	parseStart := time.Now()
	frame, err := s.parseHitsToDataFrame(hits, esTimeField, stream.dsInfo.ConfiguredFields.LogMessageField)
	parseDuration := time.Since(parseStart)
	if err != nil {
		logger.Error("fetchAndSendNewLogs: Failed to parse hits", "error", err, "duration", parseDuration)
		return fmt.Errorf("failed to parse hits to data frame: %w", err)
	}
	frame.SetMeta(&data.FrameMeta{ExecutedQueryString: "live_tail_query: " + queryDetails.Query}) // Add metadata
	logger.Debug("fetchAndSendNewLogs: DataFrame created", "rows", frame.Rows(), "duration", parseDuration)

	// 4. Send Frame and Update Timestamp
	if frame.Rows() > 0 {
		sendStart := time.Now()
		err = sender.SendFrame(frame, data.IncludeAll)
		sendDuration := time.Since(sendStart)
		if err != nil {
			logger.Error("fetchAndSendNewLogs: Failed to send frame", "error", err, "duration", sendDuration)
			return fmt.Errorf("failed to send frame: %w", err)
		}
		logger.Debug("fetchAndSendNewLogs: Sent frame successfully", "rows", frame.Rows(), "duration", sendDuration)

		// Extract the sort value from the last hit
		lastHit := hits[numHits-1]
		if lastSortValue, ok := lastHit["sort"].([]interface{}); ok && len(lastSortValue) > 0 {
			stream.mu.Lock()
			stream.lastTimestamp = lastSortValue
			logger.Debug("fetchAndSendNewLogs: Updated lastTimestamp", "new_value", stream.lastTimestamp)
			stream.mu.Unlock()
		} else {
			// Only log warning if sort field is actually present but maybe wrong type/empty
			if _, sortExists := lastHit["sort"]; sortExists {
				logger.Warn("fetchAndSendNewLogs: Received hits but failed to extract last sort value from last hit", "lastHitSortField", lastHit["sort"])
			} else {
				logger.Warn("fetchAndSendNewLogs: Received hits but sort field missing in last hit", "lastHitKeys", getMapKeys(lastHit))
			}
		}
	}

	return nil
}

// Helper function to parse ES hits into a Grafana DataFrame.
func (s *Service) parseHitsToDataFrame(hits []map[string]interface{}, timeField, messageField string) (*data.Frame, error) {
	numHits := len(hits)
	if numHits == 0 {
		return data.NewFrame("logs_live_empty"), nil
	}

	// Create fields
	timeVector := make([]*time.Time, numHits)
	bodyVector := make([]*string, numHits)
	var msgField string // Declare msgField here

	logger := s.logger // Get logger instance for use in loop

	for i, hit := range hits {
		source, ok := hit["_source"].(map[string]interface{})
		if !ok {
			logger.Warn("parseHitsToDataFrame: Hit missing _source", "hitIndex", i, "hitId", hit["_id"])
			source = make(map[string]interface{}) // Handle cases where _source might be missing?
		}

		// Extract timestamp
		if tsVal, ok := source[timeField]; ok {
			ts, err := parseTimestamp(tsVal)
			if err == nil {
				utcTime := ts.UTC()
				timeVector[i] = &utcTime
			} else {
				// Log warning only once per stream run maybe? Or rate limit?
				logger.Warn("parseHitsToDataFrame: Failed to parse timestamp in hit", "hitIndex", i, "value", tsVal, "error", err)
			}
		} else {
			logger.Warn("parseHitsToDataFrame: Timestamp field not found in source", "hitIndex", i, "expectedField", timeField)
		}

		// Extract body/message
		msgField = messageField
		if msgField == "" {
			msgField = "_source" // Fallback
		}

		var bodyStr string
		if msgField == "_source" {
			jsonBytes, err := json.Marshal(source)
			if err == nil {
				bodyStr = string(jsonBytes)
			} else {
				logger.Warn("parseHitsToDataFrame: Failed to marshal _source to string", "hitIndex", i, "error", err)
				bodyStr = "[failed to marshal source]"
			}
		} else if bodyVal, ok := source[msgField]; ok {
			bodyStr = fmt.Sprintf("%v", bodyVal) // Convert to string
		} else {
			logger.Warn("parseHitsToDataFrame: Message field not found in source", "hitIndex", i, "expectedField", msgField)
			// Leave bodyStr empty or set a placeholder?
		}
		bodyVector[i] = &bodyStr
	}

	// Use determined msgField for the frame field name
	// If msgField ended up being "_source", maybe use a more generic name like "message"?
	frameMsgField := msgField
	if frameMsgField == "_source" {
		frameMsgField = "message"
	}

	frame := data.NewFrame("logs_live",
		data.NewField(timeField, nil, timeVector),
		data.NewField(frameMsgField, nil, bodyVector),
	)

	return frame, nil
}

// PublishStream handles messages pushed from the frontend to the backend.
func (s *Service) PublishStream(ctx context.Context, req *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	// Return an error indicating not supported/implemented
	return nil, fmt.Errorf("publish stream is not implemented for Elasticsearch")
}

// --- End StreamHandler Implementation ---

func newInstanceSettings(httpClientProvider *httpclient.Provider) datasource.InstanceFactoryFunc {
	return func(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
		jsonData := map[string]any{}
		err := json.Unmarshal(settings.JSONData, &jsonData)
		if err != nil {
			return nil, fmt.Errorf("error reading settings: %w", err)
		}
		httpCliOpts, err := settings.HTTPClientOptions(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting http options: %w", err)
		}

		// Set SigV4 service namespace
		if httpCliOpts.SigV4 != nil {
			httpCliOpts.SigV4.Service = "es"
		}

		httpCli, err := httpClientProvider.New(httpCliOpts)
		if err != nil {
			return nil, err
		}

		timeField, ok := jsonData["timeField"].(string)
		if !ok {
			timeField = "" // Default to empty, validation happens in RunStream if needed
		}

		logLevelField, ok := jsonData["logLevelField"].(string)
		if !ok {
			logLevelField = ""
		}

		logMessageField, ok := jsonData["logMessageField"].(string)
		if !ok {
			logMessageField = ""
		}

		interval, ok := jsonData["interval"].(string)
		if !ok {
			interval = ""
		}

		index, ok := jsonData["index"].(string)
		if !ok {
			index = ""
		}
		if index == "" {
			index = settings.Database
		}

		var maxConcurrentShardRequests int64

		switch v := jsonData["maxConcurrentShardRequests"].(type) {
		case float64:
			maxConcurrentShardRequests = int64(v)
		case string:
			maxConcurrentShardRequests, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				maxConcurrentShardRequests = defaultMaxConcurrentShardRequests
			}
		default:
			maxConcurrentShardRequests = defaultMaxConcurrentShardRequests
		}

		if maxConcurrentShardRequests <= 0 {
			maxConcurrentShardRequests = defaultMaxConcurrentShardRequests
		}

		includeFrozen, ok := jsonData["includeFrozen"].(bool)
		if !ok {
			includeFrozen = false
		}

		configuredFields := es.ConfiguredFields{
			TimeField:       timeField,
			LogLevelField:   logLevelField,
			LogMessageField: logMessageField,
		}

		model := es.DatasourceInfo{
			ID:                         settings.ID,
			URL:                        settings.URL,
			HTTPClient:                 httpCli,
			Database:                   index,
			MaxConcurrentShardRequests: maxConcurrentShardRequests,
			ConfiguredFields:           configuredFields,
			Interval:                   interval,
			IncludeFrozen:              includeFrozen,
		}
		return model, nil
	}
}

func (s *Service) getDSInfo(ctx context.Context, pluginCtx backend.PluginContext) (*es.DatasourceInfo, error) {
	i, err := s.im.Get(ctx, pluginCtx)
	if err != nil {
		return nil, err
	}

	instance, ok := i.(es.DatasourceInfo)
	if !ok {
		return nil, fmt.Errorf("internal error: datasource instance is not of type es.DatasourceInfo")
	}

	return &instance, nil
}

func isFieldCaps(url string) bool {
	return strings.HasSuffix(url, "/_field_caps") || url == "_field_caps"
}

func (s *Service) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	logger := s.logger.FromContext(ctx)

	if req.Path != "" && !isFieldCaps(req.Path) && req.Path != "_msearch" &&
		!strings.HasSuffix(req.Path, "/_mapping") && req.Path != "_mapping" {
		logger.Error("Invalid resource path", "path", req.Path)
		return fmt.Errorf("invalid resource URL: %s", req.Path)
	}

	ds, err := s.getDSInfo(ctx, req.PluginContext)
	if err != nil {
		logger.Error("Failed to get data source info", "error", err)
		return err
	}

	esUrl, err := createElasticsearchURL(req, ds)
	if err != nil {
		logger.Error("Failed to create request url", "error", err, "url", ds.URL, "path", req.Path)
		return fmt.Errorf("failed to create request URL: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, req.Method, esUrl, bytes.NewBuffer(req.Body))
	if err != nil {
		logger.Error("Failed to create request", "error", err, "url", esUrl)
		return err
	}

	logger.Debug("Sending request to Elasticsearch", "resourcePath", req.Path)
	start := time.Now()
	response, err := ds.HTTPClient.Do(request)

	if err != nil {
		status := "error"
		if errors.Is(err, context.Canceled) {
			status = "cancelled"
		}
		lp := []any{"error", err, "status", status, "duration", time.Since(start), "stage", es.StageDatabaseRequest, "resourcePath", req.Path}
		sourceErr := backend.ErrorWithSource{}
		if errors.As(err, &sourceErr) {
			lp = append(lp, "statusSource", sourceErr.ErrorSource())
		}
		if response != nil {
			lp = append(lp, "statusCode", response.StatusCode)
		}
		logger.Error("Error received from Elasticsearch", lp...)
		if response != nil && response.Body != nil {
			_ = response.Body.Close() // Attempt to close body even on error
		}
		return err
	}

	logger.Info("Response received from Elasticsearch", "statusCode", response.StatusCode, "status", "ok", "duration", time.Since(start), "stage", es.StageDatabaseRequest, "contentLength", response.Header.Get("Content-Length"), "resourcePath", req.Path)

	defer func() {
		if response.Body != nil {
			if err := response.Body.Close(); err != nil {
				logger.Warn("Failed to close response body", "error", err)
			}
		}
	}()

	if response.Body == nil {
		return sender.Send(&backend.CallResourceResponse{
			Status: response.StatusCode,
		})
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		logger.Error("Error reading response body bytes", "error", err)
		return err
	}

	responseHeaders := map[string][]string{
		"content-type": {"application/json"},
	}

	if response.Header.Get("Content-Encoding") != "" {
		responseHeaders["content-encoding"] = []string{response.Header.Get("Content-Encoding")}
	}

	return sender.Send(&backend.CallResourceResponse{
		Status:  response.StatusCode,
		Headers: responseHeaders,
		Body:    body,
	})
}

func createElasticsearchURL(req *backend.CallResourceRequest, ds *es.DatasourceInfo) (string, error) {
	esUrl, err := url.Parse(ds.URL)
	if err != nil {
		return "", fmt.Errorf("failed to parse data source URL: %s, error: %w", ds.URL, err)
	}

	urlPathEndsSlash := strings.HasSuffix(esUrl.Path, "/")
	reqPathStartsSlash := strings.HasPrefix(req.Path, "/")

	if urlPathEndsSlash && reqPathStartsSlash {
		esUrl.Path = path.Join(esUrl.Path, req.Path[1:])
	} else {
		esUrl.Path = path.Join(esUrl.Path, req.Path)
	}

	if isFieldCaps(req.Path) {
		esUrl.RawQuery = "fields=*"
	}
	esUrlString := esUrl.String()

	if req.Path == "" && !strings.HasSuffix(esUrlString, "/") {
		return esUrlString + "/", nil
	}
	return esUrlString, nil
}

// Helper to build the actual ES query body map for fetching logs after a certain point
func buildLiveTailQueryBodyMap(luceneQuery string, lastSortValue []interface{}, timeField string) (map[string]interface{}, error) {
	body := map[string]interface{}{
		"size": defaultStreamBatchSize,
		"sort": []map[string]string{
			{timeField: "asc"},
			{"_doc": "asc"},
		},
		"_source": true,
	}

	if len(lastSortValue) > 0 {
		body["search_after"] = lastSortValue
	}

	boolQueryFilters := []map[string]interface{}{}

	if luceneQuery != "" {
		boolQueryFilters = append(boolQueryFilters, map[string]interface{}{
			"query_string": map[string]interface{}{
				"query": luceneQuery,
			},
		})
	}

	var rangeStartTime interface{}
	if len(lastSortValue) > 0 && len(lastSortValue) >= 1 {
		rangeStartTime = lastSortValue[0]
	} else {
		rangeStartTime = time.Now().Add(-5 * defaultPollInterval).UnixMilli()
	}

	boolQueryFilters = append(boolQueryFilters, map[string]interface{}{
		"range": map[string]interface{}{
			timeField: map[string]interface{}{
				"gte":    rangeStartTime,
				"format": "epoch_millis",
			},
		},
	})

	if len(boolQueryFilters) > 0 {
		body["query"] = map[string]interface{}{"bool": map[string]interface{}{"filter": boolQueryFilters}}
	} else {
		body["query"] = map[string]interface{}{"match_all": map[string]interface{}{}}
	}

	return body, nil
}

// Simple timestamp parser helper (Needs robust implementation, reuse from main query logic if possible)
func parseTimestamp(value interface{}) (time.Time, error) {
	if strVal, ok := value.(string); ok {
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02 15:04:05.000",
		}
		for _, format := range formats {
			t, err := time.Parse(format, strVal)
			if err == nil {
				return t.UTC(), nil // Return UTC
			}
		}
		// Attempt epoch millis parsing from string
		epochMillis, err := strconv.ParseInt(strVal, 10, 64)
		if err == nil {
			return time.UnixMilli(epochMillis).UTC(), nil
		}
		return time.Time{}, fmt.Errorf("unsupported string date format or value: %s", strVal)
	}
	if numVal, ok := value.(float64); ok {
		return time.UnixMilli(int64(numVal)).UTC(), nil
	}
	if intVal, ok := value.(int64); ok {
		return time.UnixMilli(intVal).UTC(), nil
	}
	if jsonNum, ok := value.(json.Number); ok {
		int64Val, err := jsonNum.Int64()
		if err == nil {
			return time.UnixMilli(int64Val).UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", value)
}

// Helper to get map keys for logging missing sort field
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
