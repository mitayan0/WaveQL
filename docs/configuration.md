# Configuration

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `WAVEQL_DATA_DIR` | Base data path | `~/.waveql/` |
| `WAVEQL_TRANSACTION_DB` | Saga Log (SQLite) | `$DATA_DIR/transactions.db` |
| `WAVEQL_REGISTRY_DB` | View Metadata (SQLite) | `$DATA_DIR/registry.db` |
| `WAVEQL_VIEWS_DIR` | Cache (Parquet) | `$DATA_DIR/views/` |
| `WAVEQL_CDC_STATE_DB` | Stream State (SQLite) | `$DATA_DIR/cdc_state.db` |
| `WAVEQL_CREDENTIALS` | Secrets File (YAML) | `$DATA_DIR/credentials.yaml` |

## Programmatic

```python
from waveql.config import WaveQLConfig, set_config

# Set global config at startup
set_config(WaveQLConfig(
    data_dir="/var/lib/waveql",
    credentials_file="/etc/secrets/waveql.yaml"
))
```

## Security
*   **Production**: Set `WAVEQL_DATA_DIR` to a persistent volume.
*   **Secrets**: Use `credentials.yaml` or env vars (`WAVEQL_AUTH_*`). Never commit secrets to code.
*   **Serverless**: Use `:memory:` path for temporary DBs if persistence isn't needed.
