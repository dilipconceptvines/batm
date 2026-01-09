# DocuSign Utility

A comprehensive Python utility for DocuSign integration with persistent storage.

## Features

- Send documents for e-signature
- Support multiple signature placeholders
- Track envelope status
- Download signed documents
- List envelopes (signed and unsigned)
- Persistent storage of envelope data
- JWT authentication
- Automatic token refresh

## Setup Instructions

1. Create a DocuSign Developer Account:
   - Go to [DocuSign Developer Center](https://developers.docusign.com/)
   - Create a new integration
   - Get your integration key (client_id) and secret

2. Configure the utility:
   - Create a config.json file with your DocuSign credentials:
     ```json
     {
       "client_id": "your_client_id",
       "account_id": "your_account_id",
       "user_id": "your_user_id",
       "private_key_file": "path/to/private.key",
       "auth_server": "account-d.docusign.com",
       "base_path": "https://demo.docusign.net/restapi"
     }
     ```
   - Store your RSA private key in the specified private key file

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

See example_usage.py for a complete example. Here's a quick start:

```python
from docusign_utility import DocuSignClient

# Initialize client
client = DocuSignClient()

# Send document for signature
envelope_id = client.send_document(
    document_path='document.pdf',
    signers=[
        {
            'email': 'signer@example.com',
            'name': 'John Doe',
            'anchor_text': '/signature/'
        }
    ]
)

# Check status
status = client.get_envelope_status(envelope_id)

# Download signed document
client.download_signed_document(envelope_id, 'signed_document.pdf')
```

## Storage

The utility uses JSON file-based storage by default. Envelope data is stored in `envelopes.json`. Each envelope record contains:
- Status
- Document name
- Signer information
- Timestamps
- Additional metadata

## Error Handling

The utility includes comprehensive error handling for:
- Authentication failures
- API errors
- File operations
- Invalid configurations

## Security

- Uses JWT authentication
- Automatic token refresh
- Secure credential storage
- No plaintext secrets in code

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

MIT License