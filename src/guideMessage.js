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
  const TITLE = "📦 HOW WE WORK";

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
          "***Leverage comes from buying together.***",
          "",
          "Follow the steps below on how to participate 👇",
          "\u200B",
          "**🧭 STEP 1 — GET ACCESS**",
          "",
          "• Request access via <#1463963353188798485>",
          "• All applicants are placed on a waitlist",
          "• Access is granted manually by admins",
          "• We prioritize serious, active buyers",
          "\u200B",
          "**📢 STEP 2 — VIEW BULK OPPORTUNITIES**",
          "",
          "In **<#1460671828593999922>** you’ll see:",
          "",
          "• Product details",
          "• Target quantities",
          "• Estimated buy price",
          "• Deadline / status",
          "",
          "Each opportunity represents a **group buy** that you can join.",
          "\u200B",
          "**📝 STEP 3 — COMMIT TO A BULK**",
          "",
          "To join a bulk:",
          "",
          "• Click the button **Join Bulk**",
          "• Enter the quantity per size",
          "• No MOQ — you can join with any quantity",
          "• Once you click **Submit**, your commitment is registered",
          "",
          "⚠️ Do not commit unless you are ready to pay.",
          "\u200B",
          "**✅ STEP 4 — BULK CONFIRMATION**",
          "",
          "A bulk is confirmed when:",
          "",
          "• The timer ends **OR**",
          "• All available pairs are fully reserved (can close earlier)",
          "",
          "Once a bulk is **CONFIRMED**:",
          "",
          "• All submitted commitments become **final**",
          "• Final pricing is shared",
          "• Payment instructions follow",
          "",
          "⚠️ Backing out after confirmation harms the group, and may result in access restrictions.",
          "\u200B",
          "**💸 STEP 5 — PAYMENT**",
          "",
          "• Every Commitment needs a **Deposit Payment** upfront",
          "• The Deposit Amount dpends on your status as a buyer (Standard is 50%)",
          "• Payments must be made within the timeframe mentioned after bulk confirmation",
          "• Late or missing payments result in:",
          "",
          "  Removal from the bulk",
          "  Possible loss of access to future opportunities",
          "\u200B",
          "**📦 STEP 6 — FULFILLMENT**",
          "",
          "After payment:",
          "",
          "• Orders are processed",
          "• Shipping & tracking follow",
          "• Updates are posted until completion",
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
