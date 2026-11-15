-- Stable physical arbiter for authored terminal-effect intents. PRE.
-- Ordinary join/loop outbox rows carry no wf_effect_key and remain unchanged.
-- The validated binding key includes flow, run_epoch, failed node and effect.
CREATE UNIQUE INDEX IF NOT EXISTS wf_outbox_terminal_effect_key_uniq
    ON "{schema}".wf_outbox ((bindings->>'wf_effect_key'))
    WHERE bindings ? 'wf_effect_key';
COMMENT ON INDEX "{schema}".wf_outbox_terminal_effect_key_uniq IS
    'At most one durable authored terminal-effect intent per flow/run_epoch/node/effect physical key.';
