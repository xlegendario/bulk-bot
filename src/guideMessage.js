import { EmbedBuilder, Events } from "discord.js";

export function registerGuideMessage(ctx) {
  const { client, env } = ctx;

  const {
    GUIDE_CHANNEL_ID,
    GUIDE_PIN_MESSAGE = "true",
  } = env;

  if (!GUIDE_CHANNEL_ID) {
    console.warn("⚠️ Guide message disabled: GUIDE_CHANNEL_ID missing.");
    return;
  }

  const SHOULD_PIN = String(GUIDE_PIN_MESSAGE).toLowerCase() === "true";
  const TITLE = "📦 How Bulk Buying Works";

  async function ensureGuideMessage() {
    const ch = await client.channels.fetch(String(GUIDE_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) {
      console.warn("⚠️ GUIDE_CHANNEL_ID is not a text channel.");
      return;
    }

    const embed = new EmbedBuilder()
      .setTitle(TITLE)
      .setDescription(
        [
          "This server is for **group bulk purchases**.",
          "Buying together = **better pricing, better leverage, lower risk**.",
          "",
          "Follow the steps below to participate 👇",
          "\u200B",
          "**🧭 Step 1 — Get Access**",
          "• Make sure you have access to **Bulk Opportunities**",
          "• If you’re new, request access via the appropriate channel",
          "• Only approved buyers can participate",
          "\u200B",
          "**📢 Step 2 — View Bulk Opportunities**",
          "In **#bulk-opportunities** you’ll see:",
          "• Product details",
          "• Target quantities",
          "• Estimated buy price",
          "• Deadline / status",
          "",
          "Each opportunity represents a **group buy**.",
          "\u200B",
          "**📝 Step 3 — Commit to a Bulk**",
          "To join a bulk:",
          "• Click the button or follow instructions in the post",
          "• Submit your **quantity commitment**",
          "• Your commitment is **binding** once confirmed",
          "",
          "⚠️ Do not commit unless you are ready to pay.",
          "\u200B",
          "**✅ Step 4 — Bulk Confirmation**",
          "Once the total quantity target is reached:",
          "• The bulk is marked **CONFIRMED**",
          "• Final pricing is shared",
          "• Payment instructions follow",
          "\u200B",
          "**💸 Step 5 — Payment**",
          "• Payment deadline will be communicated clearly",
          "• Late or missing payments may result in:",
          "  - removal from the bulk",
          "  - loss of access to future opportunities",
          "\u200B",
          "**📦 Step 6 — Fulfillment**",
          "After payment:",
          "• Orders are processed",
          "• Shipping & tracking follow",
          "• Updates are posted until completion",
          "\u200B",
          "**🤝 Important Notes**",
          "• Bulks work because **everyone commits**",
          "• Backing out hurts the group",
          "• Ask questions **before** committing",
        ].join("\n")
      )
      .setColor(0xffd300)
      .setFooter({ text: "Kickz Caviar Wholesale" });

    // Find existing message by this bot with same title
    const recent = await ch.messages.fetch({ limit: 25 }).catch(() => null);
    const existing = recent?.find(
      (m) => m.author?.id === client.user.id && m.embeds?.[0]?.title === TITLE
    );

    if (existing) {
      await existing.edit({ embeds: [embed], content: null }).catch(() => {});
      if (SHOULD_PIN && !existing.pinned) await existing.pin().catch(() => {});
      return;
    }

    const msg = await ch.send({ embeds: [embed] }).catch(() => null);
    if (msg && SHOULD_PIN) await msg.pin().catch(() => {});
  }

  client.once(Events.ClientReady, async () => {
    await ensureGuideMessage();
    console.log("✅ Guide message ensured.");
  });
}
