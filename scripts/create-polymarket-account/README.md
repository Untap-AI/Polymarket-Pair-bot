# Create Polymarket Account

Generates a new Polymarket account using ethers.js. A Polymarket account is an Ethereum wallet (EOA) on Polygon that can be used to trade on Polymarket's CLOB.

## Setup

```bash
npm install
```

## Usage

### Generate a new wallet only

```bash
node generate-account.mjs
# or
npm run generate
```

Outputs: address, private key, and mnemonic phrase.

### Generate wallet + derive API credentials

```bash
node generate-account.mjs --derive-api-key
# or
npm run derive-api-key
```

Creates a new wallet and derives L2 API credentials (apiKey, secret, passphrase) for the CLOB trading API.

### Derive API credentials from existing key

```bash
PRIVATE_KEY=0x... node generate-account.mjs --derive-api-key
```

## Security

- **Never commit** your private key or API credentials to version control
- Store secrets in environment variables or a secrets manager
- The secret and passphrase are only shown once when derived — save them immediately

### Grant token approvals (one-time setup)

Before trading, approve the exchange contracts to move your tokens:

```bash
PRIVATE_KEY=0x... node setup-approvals.mjs
# or
npm run approve
```

Requires: wallet funded with **POL** (MATIC) for gas on Polygon. Use `--dry-run` to preview without sending transactions.

## Next steps

1. Fund your wallet with **USDC.e** on Polygon (required for trading)
2. Fund your wallet with **POL** (MATIC) for gas
3. Run `setup-approvals.mjs` once to grant token approvals
4. For proxy wallets (Magic/email login), log into [polymarket.com](https://polymarket.com) once to deploy your proxy
5. Use the API credentials with the Polymarket CLOB client for programmatic trading
