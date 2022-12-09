package tarantool

import (
	"errors"
	"fmt"
)

// SessionSetting describes a connection session setting.
type SessionSetting string

const sessionSettingsSpace string = "_session_settings"

// In Go and IPROTO_UPDATE count starts with 0.
const sessionSettingValueField int = 1

const (
	// SessionErrorMarshalingEnabled defines whether error objects
	// have a special structure. Added in Tarantool 2.4.1, dropped
	// in Tarantool 2.10.0 in favor of ErrorExtensionFeature protocol
	// feature. Default is `false`.
	SessionErrorMarshalingEnabled SessionSetting = "error_marshaling_enabled"
	// SessionSQLDefaultEngine defined default storage engine for
	// new SQL tables. Added in Tarantool 2.3.1. Default is `"memtx"`.
	SessionSQLDefaultEngine SessionSetting = "sql_default_engine"
	// SessionSQLDeferForeignKeys defines whether foreign-key checks
	// can wait till commit. Added in Tarantool 2.3.1. Default is `false`.
	SessionSQLDeferForeignKeys SessionSetting = "sql_defer_foreign_keys"
	// SessionSQLFullColumnNames defines whether full column names is displayed
	// in SQL result set metadata. Added in Tarantool 2.3.1. Default is `false`.
	SessionSQLFullColumnNames SessionSetting = "sql_full_column_names"
	// SessionSQLFullMetadata defines whether SQL result set metadata will have
	// more than just name and type. Added in Tarantool 2.3.1. Default is `false`.
	SessionSQLFullMetadata SessionSetting = "sql_full_metadata"
	// SessionSQLParserDebug defines whether to show parser steps for following
	// statements. Option has no effect unless Tarantool was built with
	// `-DCMAKE_BUILD_TYPE=Debug`. Added in Tarantool 2.3.1. Default is `false`.
	SessionSQLParserDebug SessionSetting = "sql_parser_debug"
	// SessionSQLParserDebug defines whether a triggered statement can activate
	// a trigger. Added in Tarantool 2.3.1. Default is `true`.
	SessionSQLRecursiveTriggers SessionSetting = "sql_recursive_triggers"
	// SessionSQLReverseUnorderedSelects defines whether result rows are usually
	// in reverse order if there is no ORDER BY clause. Added in Tarantool 2.3.1.
	// Default is `false`.
	SessionSQLReverseUnorderedSelects SessionSetting = "sql_reverse_unordered_selects"
	// SessionSQLSelectDebug defines whether to show execution steps during SELECT.
	// Option has no effect unless Tarantool was built with  `-DCMAKE_BUILD_TYPE=Debug`.
	// Added in Tarantool 2.3.1. Default is `false`.
	SessionSQLSelectDebug SessionSetting = "sql_select_debug"
	// SessionSQLVDBEDebug defines whether VDBE debug mode is enabled.
	// Option has no effect unless Tarantool was built with  `-DCMAKE_BUILD_TYPE=Debug`.
	// Added in Tarantool 2.3.1. Default is `false`.
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

func sessionSettingValue(k SessionSetting, resp *Response) (interface{}, error) {
	if resp == nil {
		return nil, fmt.Errorf("unexpected session settings response: got nil")
	}

	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("session setting %s not found", k)
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

func wrapSessionRequestError(err error) error {
	if errors.Is(err, ClientError{Code: ErrSpaceNotFound}) {
		err = fmt.Errorf("session settings are not supported: %w", err)
	}

	return err
}

func (conn *Connection) SetSessionSetting(k SessionSetting, v interface{}) (interface{}, error) {
	req := NewUpdateRequest(sessionSettingsSpace).
		Key(sessionSettingKey{k}).
		Operations(NewOperations().Assign(sessionSettingValueField, v))

	resp, err := conn.Do(req).Get()
	if err != nil {
		return nil, wrapSessionRequestError(err)
	}

	return sessionSettingValue(k, resp)
}

func (conn *Connection) SessionSetting(k SessionSetting) (interface{}, error) {
	req := NewSelectRequest(sessionSettingsSpace).
		Key(sessionSettingKey{k}).
		Limit(1)

	resp, err := conn.Do(req).Get()
	if err != nil {
		return nil, wrapSessionRequestError(err)
	}

	return sessionSettingValue(k, resp)
}
