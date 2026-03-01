#!/usr/bin/env node
import "dotenv/config";
/**
 * One-time setup: Grant token approvals for Polymarket trading
 *
 * Before the bot can trade, your wallet must approve the exchange contracts
 * to move your tokens. Run this once before the first bot launch.
 *
 * Requires: PRIVATE_KEY in env, wallet funded with POL (MATIC) for gas
 * Optional: POLYGON_RPC_URL for custom RPC (default: publicnode)
 *
 * Usage:
 *   PRIVATE_KEY=0x... node setup-approvals.mjs
 *   PRIVATE_KEY=0x... node setup-approvals.mjs --dry-run  # Preview without sending
 */

import ethers from "ethers";

// Polymarket contract addresses (Polygon mainnet)
const ADDRESSES = {
  USDC_E: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
  CTF: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
  CTF_EXCHANGE: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
  NEG_RISK_CTF_EXCHANGE: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
};

const POLYGON_RPC =
  process.env.POLYGON_RPC_URL || "https://polygon-bor-rpc.publicnode.com";

const POLYGON_NETWORK = {
  chainId: 137,
  name: "matic",
};

const ERC20_ABI = [
  "function approve(address spender, uint256 amount) returns (bool)",
  "function allowance(address owner, address spender) view returns (uint256)",
];

const ERC1155_ABI = [
  "function setApprovalForAll(address operator, bool approved)",
  "function isApprovedForAll(address account, address operator) view returns (bool)",
];

async function getGasOverrides(provider) {
  const priorityFee = ethers.utils.parseUnits("30", "gwei"); // Polygon min ~25 gwei
  let baseFee = null;
  try {
    const feeData = await provider.getFeeData();
    baseFee = feeData.lastBaseFeePerGas;
  } catch (_) {}
  if (!baseFee || baseFee.isZero()) {
    const block = await provider.getBlock("latest");
    baseFee = block?.baseFeePerGas;
  }
  // maxFeePerGas must be >= base fee; use 2x base + priority for headroom
  const base = baseFee && !baseFee.isZero()
    ? baseFee
    : ethers.utils.parseUnits("200", "gwei"); // fallback for high congestion
  const maxFeePerGas = base.mul(2).add(priorityFee);
  return {
    maxPriorityFeePerGas: priorityFee,
    maxFeePerGas,
  };
}

async function main() {
  const privateKey = process.env.PRIVATE_KEY;
  const dryRun = process.argv.includes("--dry-run");

  if (!privateKey) {
    console.error("Error: PRIVATE_KEY environment variable is required.");
    console.error("Usage: PRIVATE_KEY=0x... node setup-approvals.mjs");
    process.exit(1);
  }

  const provider = new ethers.providers.StaticJsonRpcProvider(
    POLYGON_RPC,
    POLYGON_NETWORK
  );
  const wallet = new ethers.Wallet(privateKey, provider);

  console.log("Polymarket Token Approval Setup");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("Wallet:", wallet.address);
  console.log("Network: Polygon mainnet");
  if (dryRun) console.log("Mode: DRY RUN (no transactions will be sent)\n");
  else console.log("");

  const usdc = new ethers.Contract(ADDRESSES.USDC_E, ERC20_ABI, wallet);
  const ctf = new ethers.Contract(ADDRESSES.CTF, ERC1155_ABI, wallet);

  const maxUint256 = ethers.constants.MaxUint256;
  const gasOverrides = !dryRun ? await getGasOverrides(provider) : {};

  // 1. Approve USDC.e → CTF Contract
  console.log("1. USDC.e → CTF Contract");
  const usdcAllowance = await usdc.allowance(wallet.address, ADDRESSES.CTF);
  if (usdcAllowance.gte(maxUint256)) {
    console.log("   ✓ Already approved (unlimited)\n");
  } else {
    if (dryRun) {
      console.log("   [DRY RUN] Would call: approve(CTF, max)\n");
    } else {
      const tx = await usdc.approve(ADDRESSES.CTF, maxUint256, gasOverrides);
      console.log("   Tx:", tx.hash);
      await tx.wait();
      console.log("   ✓ Approved\n");
    }
  }

  // 2. Approve CTF outcome tokens → CTF Exchange
  console.log("2. CTF outcome tokens → CTF Exchange");
  const ctfExchangeApproved = await ctf.isApprovedForAll(
    wallet.address,
    ADDRESSES.CTF_EXCHANGE
  );
  if (ctfExchangeApproved) {
    console.log("   ✓ Already approved\n");
  } else {
    if (dryRun) {
      console.log("   [DRY RUN] Would call: setApprovalForAll(CTF_EXCHANGE, true)\n");
    } else {
      const tx = await ctf.setApprovalForAll(
        ADDRESSES.CTF_EXCHANGE,
        true,
        gasOverrides
      );
      console.log("   Tx:", tx.hash);
      await tx.wait();
      console.log("   ✓ Approved\n");
    }
  }

  // 3. Approve CTF outcome tokens → Neg Risk CTF Exchange
  console.log("3. CTF outcome tokens → Neg Risk CTF Exchange");
  const negRiskApproved = await ctf.isApprovedForAll(
    wallet.address,
    ADDRESSES.NEG_RISK_CTF_EXCHANGE
  );
  if (negRiskApproved) {
    console.log("   ✓ Already approved\n");
  } else {
    if (dryRun) {
      console.log("   [DRY RUN] Would call: setApprovalForAll(NEG_RISK_CTF_EXCHANGE, true)\n");
    } else {
      const tx = await ctf.setApprovalForAll(
        ADDRESSES.NEG_RISK_CTF_EXCHANGE,
        true,
        gasOverrides
      );
      console.log("   Tx:", tx.hash);
      await tx.wait();
      console.log("   ✓ Approved\n");
    }
  }

  console.log("═══════════════════════════════════════════════════════════");
  if (dryRun) {
    console.log("Dry run complete. Run without --dry-run to execute.");
  } else {
    console.log("All approvals complete. You can now trade on Polymarket.");
  }
  console.log("═══════════════════════════════════════════════════════════");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
