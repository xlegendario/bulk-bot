import { EmbedBuilder, Events } from "discord.js";

export function registerWelcome(ctx) {
  const { client, env } = ctx;

  const { WELCOME_CHANNEL_ID } = env;

  if (!WELCOME_CHANNEL_ID) {
    console.warn("⚠️ Welcome disabled: missing WELCOME_CHANNEL_ID");
    return;
  }

  async function ensureWelcomeMessage() {
    const ch = await client.channels.fetch(String(WELCOME_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) return;

    const embed = new EmbedBuilder()
      .setTitle("👋 Welcome to Kickz Caviar Wholesale")
      .setDescription(
        [
          "We help buyers get better prices by **buying together** and opening access to **trusted suppliers**.",
          "",
          "Leverage comes from buying together.",
          "",
          "Instead of sourcing alone, buyers combine demand — unlocking discounts that usually require large capital or long-standing supplier relationships.",
          "",
          "**Why we exist**",
          "Our mission is to make bulk discounts and global supplier access available to **everyone** — ",
          "from established resellers to newer buyers who want access to opportunities that are usually out of reach.",
          "",
          "We do this by structuring **group bulk buying** in a transparent, controlled way.",
          "",
          "**How it works**",
          "• We post **supplier-backed bulk opportunities** (SKU, size range, price, ETA)",
          "• Buyers join with their desired sizes & quantities",
          "• Combined volume determines pricing discount tiers",
          "• Prices lock when a bulk closes",
          "",
          "**What makes this different**",
          "• No individual MOQ's, members buy together as a whole",
          "• Volume discounts are unlocked collectively",
          "• No blind prepayments",
          "• Clear timelines and rules",
          "• Real supplier relationships behind every bulk",
          "• Built for buyers",
          "• No logistics hassle — we manage everything from purchase to delivery",
          "",
          "**Request a bulk**",
          "Looking for a specific SKU or model?",
          "You can request new bulk opportunities in **<#1460674030213533726>**.",
          "Requests are reviewed with our supplier network."
          "If supplier terms and group demand match, the opportunity is posted publicly.",
          "",
          "**Get started**",
          "👉 Head to **<#1460671828593999922>** to view active bulks",
          "👉 Click **Join Bulk** to open your private cart",
          "",
          "_Only submitted pairs count. Prices lock when a bulk closes._",
        ].join("\n")
      )
      .setColor(0xffd300)
      .setImage("https://i.imgur.com/xEPsW7y.png");


    const recent = await ch.messages.fetch({ limit: 25 }).catch(() => null);
    const existing = recent?.find(
      (m) =>
        m.author?.id === client.user.id &&
        m.embeds?.[0]?.title === "👋 Welcome to Kickz Caviar Wholesale"
    );

    if (existing) {
      await existing.edit({ embeds: [embed] }).catch(() => {});
    } else {
      await ch.send({ embeds: [embed] }).catch(() => {});
    }
  }

  client.once(Events.ClientReady, async () => {
    await ensureWelcomeMessage();
    console.log("👋 Welcome message ensured");
  });
}
