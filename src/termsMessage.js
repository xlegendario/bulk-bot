import { EmbedBuilder, Events } from "discord.js";

export function registerTermsMessage(ctx) {
  const { client, env } = ctx;

  const {
    TERMS_CHANNEL_ID,
    TERMS_PIN_MESSAGE = "true",
  } = env;

  if (!TERMS_CHANNEL_ID) {
    console.warn("⚠️ Terms message disabled: TERMS_CHANNEL_ID missing.");
    return;
  }

  const SHOULD_PIN = String(TERMS_PIN_MESSAGE).toLowerCase() === "true";
  const TITLE = "📜 Bulk Participation Terms";

  async function ensureTermsMessage() {
    const ch = await client.channels.fetch(String(TERMS_CHANNEL_ID)).catch(() => null);
    if (!ch || !ch.isTextBased()) {
      console.warn("⚠️ TERMS_CHANNEL_ID is not a text channel.");
      return;
    }

    const embed = new EmbedBuilder()
      .setTitle(TITLE)
      .setDescription(
        [
          "By participating in any bulk opportunity, you agree to the following terms:",
          "\u200B",
          "**1️⃣ Commitment Is Binding**",
          "• Submitting a commitment means **you agree to purchase**",
          "• Commitments cannot be withdrawn after confirmation",
          "• Do not commit “just to see”",
          "\u200B",
          "**2️⃣ Pricing & Availability**",
          "• Prices are based on **group volume**",
          "• Pricing may change if quantities change or suppliers adjust",
          "• Final pricing is confirmed **before payment**",
          "\u200B",
          "**3️⃣ Payments**",
          "• Payments must be made **on time**",
          "• Failure to pay may result in removal and access restrictions",
          "\u200B",
          "**4️⃣ Risk & Delays**",
          "• External suppliers + logistics can cause delays",
          "• We communicate transparently, but delays do not justify chargebacks",
          "\u200B",
          "**5️⃣ No Chargebacks**",
          "• Chargebacks harm the entire group",
          "• Attempted chargebacks may result in a permanent ban and recovery actions",
          "\u200B",
          "**6️⃣ Access & Conduct**",
          "• Access is **a privilege**",
          "• Bad faith actions or disruption are not tolerated",
          "• Staff decisions are final",
          "\u200B",
          "**7️⃣ Affiliate & Rewards**",
          "• Rewards are paid only for **qualified referrals**",
          "• Abuse of the system results in removal from the program",
          "\u200B",
          "**📌 Final Note**",
          "This is a professional buying environment.",
          "Respect the process and the group.",
        ].join("\n")
      )
      .setColor(0xffd300)
      .setFooter({ text: "Kickz Caviar Wholesale" });

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
    await ensureTermsMessage();
    console.log("✅ Terms message ensured.");
  });
}
