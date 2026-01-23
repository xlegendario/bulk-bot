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
          "Kickz Caviar helps buyers get better prices by buying together and working directly with trusted suppliers.",
          "",
          "**How it works:**",
          "• We post bulk opportunities (SKU, sizes, price, ETA)",
          "• Buyers join with their quantities",
          "• Bigger total volume = better pricing",
          "",
          "**What to do next:**",
          "👉 Go to **#bulk-opportunities** to join active bulks",
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
