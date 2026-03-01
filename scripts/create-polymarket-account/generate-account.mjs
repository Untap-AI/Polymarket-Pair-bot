#!/usr/bin/env node
import "dotenv/config";
/**
 * Generate a Polymarket account using ethers.js
 *
 * Creates a new Ethereum wallet (EOA) that can be used as a Polymarket account.
 * Optionally derives API credentials for the CLOB trading API.
 *
 * Usage:
 *   node generate-account.mjs                    # Generate new wallet only
 *   node generate-account.mjs --derive-api-key  # Generate wallet + derive API creds
 *   PRIVATE_KEY=0x... node generate-account.mjs --derive-api-key  # Derive from existing key
 */

import { Wallet } from "ethers";
import { ClobClient } from "@polymarket/clob-client";

const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137; // Polygon mainnet

async function main() {
  const deriveApiKey = process.argv.includes("--derive-api-key");
  const existingKey = process.env.PRIVATE_KEY;

  let wallet;

  if (existingKey) {
    // Use existing private key
    wallet = new Wallet(existingKey);
    console.log("Using existing wallet from PRIVATE_KEY env var\n");
  } else {
    // Generate new wallet
    wallet = Wallet.createRandom();
    console.log("Generated new Polymarket account\n");
  }

  console.log("═══════════════════════════════════════════════════════════");
  console.log("POLYMARKET ACCOUNT");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("Address:    ", wallet.address);
  console.log("Private Key:", wallet.privateKey);

  if (wallet.mnemonic?.phrase) {
    console.log("Mnemonic:   ", wallet.mnemonic.phrase);
  }

  console.log("═══════════════════════════════════════════════════════════\n");

  console.log("⚠️  SECURITY: Store your private key securely. Never share it or commit to git.\n");

  if (deriveApiKey) {
    console.log("Deriving Polymarket API credentials (L2 auth)...\n");

    try {
      const client = new ClobClient(CLOB_HOST, CHAIN_ID, wallet);
      const credentials = await client.createOrDeriveApiKey();

      console.log("═══════════════════════════════════════════════════════════");
      console.log("API CREDENTIALS (for CLOB trading)");
      console.log("═══════════════════════════════════════════════════════════");
      console.log("POLY_API_KEY:    ", credentials.key);
      console.log("POLY_SECRET:     ", credentials.secret);
      console.log("POLY_PASSPHRASE: ", credentials.passphrase);
      console.log("═══════════════════════════════════════════════════════════\n");

      console.log("Add to your .env file:");
      console.log(`PRIVATE_KEY=${wallet.privateKey}`);
      console.log(`POLY_API_KEY=${credentials.key}`);
      console.log(`POLY_SECRET=${credentials.secret}`);
      console.log(`POLY_PASSPHRASE=${credentials.passphrase}`);
      console.log("\n⚠️  The secret and passphrase are shown once. Save them securely.");
    } catch (err) {
      console.error("Failed to derive API credentials:", err.message);
      if (err.message?.includes("Invalid Funder")) {
        console.error("\nNote: You may need to log into polymarket.com once to deploy your proxy wallet.");
      }
      process.exit(1);
    }
  } else {
    console.log("To derive API credentials for trading, run:");
    console.log("  PRIVATE_KEY=<your-key> node generate-account.mjs --derive-api-key");
    console.log("\nOr generate a new account with API creds:");
    console.log("  node generate-account.mjs --derive-api-key");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
