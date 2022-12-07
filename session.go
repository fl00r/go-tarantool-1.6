package tarantool

import (
	"fmt"
)

type SessionSetting string

const sessionSettingsSpace string = "_session_settings"

// In Go and IPROTO count start with 0.
const sessionSettingValueField int = 1

const (
	// whether error objects have a special structure. Default = false.
	SessionErrorMarshalingEnabled SessionSetting = "error_marshaling_enabled"
	// default storage engine for new SQL tables. Default = ‘memtx’.
	SessionSQLDefaultEngine SessionSetting = "sql_default_engine"
	// whether foreign-key checks can wait till commit. Default = false.
	SessionSQLDeferForeignKeys SessionSetting = "sql_defer_foreign_keys"
	// no effect at this time. Default = false.
	SessionSQLFullColumnNames SessionSetting = "sql_full_column_names"
	// whether SQL result set metadata will have more than just name and type. Default = false.
	SessionSQLFullMetadata SessionSetting = "sql_full_metadata"
	// whether to show parser steps for following statements. Default = false.
	SessionSQLParserDebug SessionSetting = "sql_parser_debug"
	// whether a triggered statement can activate a trigger. Default = true.
	SessionSQLRecursiveTriggers SessionSetting = "sql_recursive_triggers"
	// whether result rows are usually in reverse order if there is no ORDER BY clause. Default = false.
	SessionSQLReverseUnorderedSelects SessionSetting = "sql_reverse_unordered_selects"
	// whether to show execution steps during SELECT. Default = false.
	SessionSQLSelectDebug SessionSetting = "sql_select_debug"
	// for use by Tarantool’s developers. Default = false.
	SessionSQLVDBEDebug SessionSetting = "sql_vdbe_debug"
)

type sessionSettingKey struct {
	S SessionSetting
}

func (k sessionSettingKey) EncodeMsgpack(enc *encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeString(string(k.S))
	return nil
}

func getSessionSettingValue(resp *Response) (interface{}, error) {
	if resp == nil {
		return nil, fmt.Errorf("unexpected session settings response: got nil")
	}

	if len(resp.Data) != 1 {
		return nil, fmt.Errorf("unexpected session settings response length %d",
			len(resp.Data))
	}

	tuple, ok := resp.Data[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected session settings response format: expected tuple, got %v",
			resp.Data)
	}

	// (key, value) tuple expected
	if len(tuple) < 2 {
		return nil, fmt.Errorf("unexpected session settings response format: too few fields in the tuple (got %v)",
			resp.Data)
	}

	return tuple[sessionSettingValueField], nil
}

func (conn *Connection) SetSessionSetting(k SessionSetting, v interface{}) (interface{}, error) {
	req := NewUpdateRequest(sessionSettingsSpace).
		Key(sessionSettingKey{k}).
		Operations(NewOperations().Assign(sessionSettingValueField, v))

	resp, err := conn.Do(req).Get()
	if err != nil {
		return nil, err
	}

	return getSessionSettingValue(resp)
}

func (conn *Connection) GetSessionSetting(k SessionSetting) (interface{}, error) {
	req := NewSelectRequest(sessionSettingsSpace).
		Key(sessionSettingKey{k}).
		Limit(1)

	resp, err := conn.Do(req).Get()
	if err != nil {
		return nil, err
	}

	return getSessionSettingValue(resp)
}
