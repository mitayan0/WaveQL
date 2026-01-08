
import sys
import json

def discover():
    catalog = {
        "streams": [
            {
                "stream": "users",
                "tap_stream_id": "users",
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"}
                    }
                }
            }
        ]
    }
    print(json.dumps(catalog))

def sync():
    # Emit schema
    print(json.dumps({
        "type": "SCHEMA",
        "stream": "users",
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }
        },
        "key_properties": ["id"]
    }))
    
    # Emit records
    print(json.dumps({
        "type": "RECORD",
        "stream": "users",
        "record": {"id": 1, "name": "Alice"}
    }))
    print(json.dumps({
        "type": "RECORD",
        "stream": "users",
        "record": {"id": 2, "name": "Bob"}
    }))
    
    # Emit state
    print(json.dumps({
        "type": "STATE",
        "value": {"users": 2}
    }))

if __name__ == "__main__":
    if "--discover" in sys.argv:
        discover()
    else:
        sync()
