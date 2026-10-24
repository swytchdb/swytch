package redis

import (
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

const (
	clusterSafeModeConfig = "cluster_safe_mode"
	keySafeModeConfig     = "key_safe_mode"
)

// Config remains a proxy: the engine keys are the source of truth, including
// writes made directly with SET/HSET/HDEL/DEL by ordinary clients.
func (h *Handler) safetyConfigGet(name string) (string, error) {
	if h.engine == nil {
		if name == clusterSafeModeConfig {
			return "safe", nil
		}

		return "{}", nil
	}

	key := effects.ClusterSafetyKey
	if name == keySafeModeConfig {
		key = effects.KeySafetyKey
	}

	state, _, _, err := h.engine.GetSnapshot(key)
	if err != nil {
		return "", err
	}

	if name == clusterSafeModeConfig {
		mode, err := effects.ClusterSafetyMode(state)

		return mode.String(), err
	}

	modes, err := effects.KeySafetyModes(state)
	if err != nil {
		return "", err
	}

	values := make(map[string]string, len(modes))
	for pattern, mode := range modes {
		if !utf8.ValidString(pattern) {
			return "", fmt.Errorf("key_safe_mode JSON cannot represent a non-UTF-8 pattern; read __swytch::kc directly")
		}

		values[pattern] = mode.String()
	}

	encoded, err := json.Marshal(values)

	return string(encoded), err
}

// Validate the whole policy update before emitting, so an invalid later field
// cannot leave a partially replaced configuration behind.
func safetyConfigEffects(args [][]byte) ([]*pb.Effect, error) {
	var result []*pb.Effect

	for i := 0; i < len(args); i += 2 {
		name, value := string(args[i]), args[i+1]
		switch name {
		case clusterSafeModeConfig:
			if _, err := effects.ParseSafetyMode(string(value)); err != nil {
				return nil, err
			}

			result = append(result, configRemove(effects.ClusterSafetyKey), configType(effects.ClusterSafetyKey, pb.ValueType_TYPE_STRING), &pb.Effect{
				Key: []byte(effects.ClusterSafetyKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS, Collection: pb.CollectionKind_SCALAR,
					Value: &pb.DataEffect_Raw{Raw: value},
				}},
			})
		case keySafeModeConfig:
			var modes map[string]string

			if !utf8.Valid(value) {
				return nil, fmt.Errorf("key_safe_mode requires UTF-8 JSON")
			}

			if err := json.Unmarshal(value, &modes); err != nil || modes == nil {
				return nil, fmt.Errorf("key_safe_mode requires a JSON object mapping patterns to safe or unsafe")
			}

			patterns := make([]string, 0, len(modes))
			for pattern, mode := range modes {
				if _, err := effects.ParseSafetyMode(mode); err != nil {
					return nil, fmt.Errorf("key_safe_mode pattern %q: %w", pattern, err)
				}

				patterns = append(patterns, pattern)
			}

			sort.Strings(patterns)

			result = append(result, configRemove(effects.KeySafetyKey), configType(effects.KeySafetyKey, pb.ValueType_TYPE_HASH))
			for _, pattern := range patterns {
				result = append(result, &pb.Effect{Key: []byte(effects.KeySafetyKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS, Collection: pb.CollectionKind_KEYED,
					Id: []byte(pattern), Value: &pb.DataEffect_Raw{Raw: []byte(modes[pattern])},
				}}})
			}
		}
	}

	return result, nil
}

func configRemove(key string) *pb.Effect {
	return &pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}}}
}

func configType(key string, tag pb.ValueType) *pb.Effect {
	return &pb.Effect{Key: []byte(key), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: tag}}}
}

func (h *Handler) writeSafetyConfig(updates []*pb.Effect) error {
	if len(updates) == 0 {
		return nil
	}

	if h.engine == nil {
		return fmt.Errorf("safety configuration requires the effects engine")
	}

	ctx := h.engine.NewContext()
	for _, update := range updates {
		if err := ctx.Emit(update); err != nil {
			return err
		}
	}

	return ctx.Flush()
}
