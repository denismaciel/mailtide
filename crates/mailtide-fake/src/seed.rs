//! Deterministic generic synthetic corpus generator and `.eml` folder importer.

use std::collections::BTreeMap;
use std::path::Path;

use mailtide_mime::{Address, Attachment, ComposeOptions, MessageId, OutgoingMessage, Purpose};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::state::{AccountState, now_ms};

/// Parameters for generating a deterministic synthetic mailbox corpus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedSpec {
    /// Number of threads to generate.
    #[serde(default = "default_threads")]
    pub threads: usize,
    /// Deterministic PRNG seed.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Maximum messages per thread (actual depth varies 1..=max).
    #[serde(default = "default_max_depth")]
    pub max_messages_per_thread: usize,
    /// Fraction of messages containing HTML + plain multipart (0.0..=1.0).
    #[serde(default = "default_html_ratio")]
    pub html_ratio: f32,
    /// Fraction of threads containing attachments (0.0..=1.0).
    #[serde(default = "default_attachment_ratio")]
    pub attachment_ratio: f32,
    /// Fraction of threads marked `UNREAD` (0.0..=1.0).
    #[serde(default = "default_unread_ratio")]
    pub unread_ratio: f32,
    /// Fraction of threads marked `STARRED` (0.0..=1.0).
    #[serde(default = "default_starred_ratio")]
    pub starred_ratio: f32,
    /// Number of drafts to generate (mix of standalone, reply, rich HTML, and BCC).
    #[serde(default = "default_drafts")]
    pub draft_count: usize,
    /// User labels to create and distribute across threads.
    #[serde(default = "default_labels")]
    pub labels: Vec<String>,
    /// Spread timestamps backwards over this many days.
    #[serde(default = "default_span_days")]
    pub span_days: u32,
    /// If true, clear the account before seeding.
    #[serde(default)]
    pub clear_first: bool,
}

fn default_threads() -> usize {
    40
}
fn default_seed() -> u64 {
    42
}
fn default_max_depth() -> usize {
    6
}
fn default_html_ratio() -> f32 {
    0.55
}
fn default_attachment_ratio() -> f32 {
    0.25
}
fn default_unread_ratio() -> f32 {
    0.25
}
fn default_starred_ratio() -> f32 {
    0.15
}
fn default_drafts() -> usize {
    4
}
fn default_span_days() -> u32 {
    45
}
fn default_labels() -> Vec<String> {
    vec![
        "Projects/Alpha".to_owned(),
        "Projects/Beta".to_owned(),
        "Finance/Invoices".to_owned(),
        "Team/Updates".to_owned(),
        "Follow-up".to_owned(),
    ]
}

impl Default for SeedSpec {
    fn default() -> Self {
        Self {
            threads: default_threads(),
            seed: default_seed(),
            max_messages_per_thread: default_max_depth(),
            html_ratio: default_html_ratio(),
            attachment_ratio: default_attachment_ratio(),
            unread_ratio: default_unread_ratio(),
            starred_ratio: default_starred_ratio(),
            draft_count: default_drafts(),
            labels: default_labels(),
            span_days: default_span_days(),
            clear_first: false,
        }
    }
}

/// Summary returned after seeding an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedResult {
    /// Threads created.
    pub threads_created: usize,
    /// Messages created.
    pub messages_created: usize,
    /// Drafts created.
    pub drafts_created: usize,
    /// Labels ensured.
    pub labels_created: usize,
    /// Final `historyId`.
    pub history_id: u64,
}

/// Simple deterministic xorshift64* PRNG (no extra crate needed).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn usize(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % bound
    }

    fn chance(&mut self, ratio: f32) -> bool {
        let clamped = ratio.clamp(0.0, 1.0);
        let draw = (self.next_u64() % 10_000) as f32 / 10_000.0;
        draw < clamped
    }
}

const CONTACTS: &[(&str, &str)] = &[
    ("Elena Morales", "elena@northwind.example"),
    ("Marcus Chen", "marcus.chen@acme-labs.example"),
    ("Priya Nair", "priya@solstice.example"),
    ("Jonas Lindqvist", "jonas@fjord-tech.example"),
    ("Hannah Brooks", "hannah@brightleaf.example"),
    ("Devon Patel", "devon@cobalt-works.example"),
    ("Claire Dubois", "claire@atelier-west.example"),
    ("Samir Haddad", "samir@cedar-ops.example"),
    ("Tessa Gallagher", "tessa@harborline.example"),
    ("Kenji Takahashi", "kenji@kumo-systems.example"),
    ("Billing Notices", "billing@cloud-vendor.example"),
    ("Weekly Digest", "digest@engineering-news.example"),
];

const SUBJECTS: &[&str] = &[
    "Q4 roadmap milestone review",
    "Updated invoice #4821 for October services",
    "Architecture notes for the sync pipeline",
    "Schedule check-in for next Tuesday",
    "Design critique: navigation & keyboard shortcuts",
    "Action needed: confirm shipping details",
    "Follow-up on performance benchmarks",
    "Draft contract redlines attached",
    "Quarterly budget reconciliation",
    "Release checklist and rollout window",
    "Customer feedback summary from beta cohort",
    "Onboarding guide updates for new team members",
];

const PARAGRAPHS: &[&str] = &[
    "Thanks for sending over the latest numbers. I reviewed the summary and left a few notes inline so we can finalize before Friday.",
    "Could you double-check section 3? The timeline looks realistic, though we may want an extra buffer day around the migration step.",
    "Attached is the revised document with the adjustments we discussed yesterday. Let me know if anything else needs attention.",
    "Quick heads up: we noticed a small spike in latency during peak hours and pushed a configuration tweak that brought p95 back down.",
    "Looks great on my end! Once everyone signs off, we can move forward with the rollout plan.",
    "Adding a couple of teammates on CC so they have visibility into the updated deliverables and milestones.",
];

const LABEL_COLORS: &[(&str, &str)] = &[
    ("#ffffff", "#4a86e8"),
    ("#ffffff", "#16a766"),
    ("#000000", "#fad165"),
    ("#ffffff", "#fb4c2f"),
    ("#ffffff", "#a479e2"),
];

/// Minimal valid 1x1 PNG image for inline CID / image attachments.
const TINY_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
    0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08, 0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00,
    0x00, 0x03, 0x01, 0x01, 0x00, 0x18, 0xDD, 0x8D, 0xB0, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E,
    0x44, 0xAE, 0x42, 0x60, 0x82,
];

/// Minimal valid PDF document for realistic PDF attachment previews/downloads.
fn sample_pdf(title: &str) -> Vec<u8> {
    format!(
        "%PDF-1.4\n1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n\
         2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n\
         3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >> endobj\n\
         4 0 obj << /Length 44 >> stream\nBT /F1 12 Tf 72 720 Td ({title}) Tj ET\nendstream endobj\n\
         trailer << /Root 1 0 R >>\n%%EOF\n"
    )
    .into_bytes()
}

/// Populate `account` with deterministic realistic threads, messages, labels, and drafts.
pub fn populate(account: &mut AccountState, spec: &SeedSpec) -> SeedResult {
    if spec.clear_first {
        let email = account.email.clone();
        *account = AccountState::new(email);
    }

    let mut rng = Rng::new(spec.seed);
    let mut user_label_ids = Vec::with_capacity(spec.labels.len());
    for (idx, name) in spec.labels.iter().enumerate() {
        let (fg, bg) = LABEL_COLORS[idx % LABEL_COLORS.len()];
        let lid = account.ensure_label(
            name,
            Some(json!({ "textColor": fg, "backgroundColor": bg })),
        );
        user_label_ids.push(lid);
    }

    let base_now = now_ms();
    let span_ms = i64::from(spec.span_days.max(1)) * 86_400_000;
    let max_depth = spec.max_messages_per_thread.max(1);
    let mut messages_created = 0_usize;
    let mut recent_threads: Vec<(String, String, String, String)> = Vec::new();

    for t_idx in 0..spec.threads {
        let (sender_name, sender_email) = CONTACTS[rng.usize(CONTACTS.len())];
        let base_subject = SUBJECTS[t_idx % SUBJECTS.len()];
        let subject = if t_idx >= SUBJECTS.len() {
            format!("{base_subject} ({})", t_idx + 1)
        } else {
            base_subject.to_owned()
        };
        let depth = 1 + rng.usize(max_depth);
        let thread_start = base_now - (rng.next_u64() as i64).abs() % span_ms;
        let is_unread = rng.chance(spec.unread_ratio);
        let is_starred = rng.chance(spec.starred_ratio);
        let is_archived = rng.chance(0.18);
        let is_trash = !is_archived && rng.chance(0.03);
        let is_spam = !is_archived && !is_trash && rng.chance(0.02);
        let thread_label = if !user_label_ids.is_empty() && rng.chance(0.45) {
            Some(user_label_ids[rng.usize(user_label_ids.len())].clone())
        } else {
            None
        };
        let thread_has_att = rng.chance(spec.attachment_ratio);

        let mut thread_id: Option<String> = None;
        let mut refs: Vec<MessageId> = Vec::new();
        let mut prev_body = String::new();

        for m_idx in 0..depth {
            let msg_time = (thread_start + (m_idx as i64) * 3_600_000).min(base_now - 1000);
            let from_self = m_idx % 2 == 1;
            let from_addr = if from_self {
                Address {
                    name: Some("Account Owner".to_owned()),
                    email: account.email.clone(),
                }
            } else {
                Address {
                    name: Some(sender_name.to_owned()),
                    email: sender_email.to_owned(),
                }
            };
            let to_addr = if from_self {
                Address {
                    name: Some(sender_name.to_owned()),
                    email: sender_email.to_owned(),
                }
            } else {
                Address {
                    name: None,
                    email: account.email.clone(),
                }
            };

            let mut outgoing = OutgoingMessage::new(from_addr, msg_time.div_euclid(1000));
            outgoing.to.push(to_addr);
            if rng.chance(0.2) {
                let (cc_name, cc_email) = CONTACTS[rng.usize(CONTACTS.len())];
                if cc_email != sender_email && cc_email != account.email {
                    outgoing.cc.push(Address {
                        name: Some(cc_name.to_owned()),
                        email: cc_email.to_owned(),
                    });
                }
            }
            outgoing.subject = Some(if m_idx == 0 {
                subject.clone()
            } else {
                format!("Re: {subject}")
            });
            let msg_rfc_id = format!("<seed-{}-{t_idx}-{m_idx}@mail.example>", spec.seed);
            if let Ok(parsed_id) = MessageId::parse(&msg_rfc_id) {
                outgoing.message_id = Some(parsed_id.clone());
                if let Some(last) = refs.last() {
                    outgoing.in_reply_to = Some(last.clone());
                    outgoing.references.clone_from(&refs);
                }
                refs.push(parsed_id);
            }

            let p1 = PARAGRAPHS[rng.usize(PARAGRAPHS.len())];
            let p2 = PARAGRAPHS[rng.usize(PARAGRAPHS.len())];
            let body_text = if prev_body.is_empty() {
                format!("Hi,\n\n{p1}\n\n{p2}\n\nBest,\n{sender_name}")
            } else {
                format!(
                    "Thanks,\n\n{p1}\n\n> {}",
                    prev_body.lines().take(3).collect::<Vec<_>>().join("\n> ")
                )
            };
            p1.clone_into(&mut prev_body);
            outgoing.text = Some(body_text.clone());

            if rng.chance(spec.html_ratio) {
                let include_cid = thread_has_att && m_idx == 0 && rng.chance(0.35);
                let img_tag = if include_cid {
                    outgoing.inline.push(Attachment {
                        filename: Some("logo.png".to_owned()),
                        content_type: "image/png".to_owned(),
                        content_id: Some("inline-logo@example".to_owned()),
                        bytes: TINY_PNG.to_vec(),
                    });
                    r#"<p><img src="cid:inline-logo@example" alt="logo" width="16" height="16"/></p>"#
                } else {
                    ""
                };
                outgoing.html = Some(format!(
                    "<div style=\"font-family:sans-serif\"><p>{p1}</p><p>{p2}</p>{img_tag}</div>"
                ));
            }

            if thread_has_att && m_idx == 0 {
                if t_idx % 2 == 0 {
                    outgoing.attachments.push(Attachment {
                        filename: Some(format!("document-{}.pdf", t_idx + 1)),
                        content_type: "application/pdf".to_owned(),
                        content_id: None,
                        bytes: sample_pdf(&format!("Document {}", t_idx + 1)),
                    });
                } else {
                    outgoing.attachments.push(Attachment {
                        filename: Some(format!("report-{}.csv", t_idx + 1)),
                        content_type: "text/csv".to_owned(),
                        content_id: None,
                        bytes: format!(
                            "item,amount,status\nalpha,{},ok\nbeta,{},pending\n",
                            (t_idx + 1) * 100,
                            (t_idx + 1) * 45
                        )
                        .into_bytes(),
                    });
                }
            }

            let raw = mailtide_mime::compose(
                &outgoing,
                &ComposeOptions {
                    purpose: Purpose::Draft,
                    max_bytes: None,
                },
            )
            .map_or_else(
                |_| {
                    format!("From: {sender_email}\r\nSubject: {subject}\r\n\r\n{body_text}\r\n")
                        .into_bytes()
                },
                |c| c.raw,
            );

            let mut labels = Vec::new();
            if from_self {
                labels.push("SENT".to_owned());
            } else if is_trash {
                labels.push("TRASH".to_owned());
            } else if is_spam {
                labels.push("SPAM".to_owned());
            } else if !is_archived {
                labels.push("INBOX".to_owned());
            }
            if is_unread && m_idx + 1 == depth && !from_self {
                labels.push("UNREAD".to_owned());
            }
            if is_starred && m_idx + 1 == depth {
                labels.push("STARRED".to_owned());
            }
            if let Some(ref lid) = thread_label {
                labels.push(lid.clone());
            }

            let (stored, _) =
                account.add_message(None, thread_id.clone(), labels, Some(msg_time), raw);
            if thread_id.is_none() {
                thread_id = Some(stored.thread_id.clone());
            }
            messages_created += 1;
            if m_idx + 1 == depth && recent_threads.len() < 16 {
                recent_threads.push((
                    stored.thread_id,
                    sender_email.to_owned(),
                    subject.clone(),
                    stored.rfc822_message_id,
                ));
            }
        }
    }

    let mut drafts_created = 0_usize;
    for d_idx in 0..spec.draft_count {
        let mut outgoing = OutgoingMessage::new(
            Address {
                name: Some("Account Owner".to_owned()),
                email: account.email.clone(),
            },
            now_ms().div_euclid(1000),
        );
        let thread = if d_idx < recent_threads.len() {
            let (tid, recipient, subj, irt) = &recent_threads[d_idx];
            outgoing.to.push(Address {
                name: None,
                email: recipient.clone(),
            });
            outgoing.subject = Some(format!("Re: {subj}"));
            if let Ok(parsed) = MessageId::parse(irt) {
                outgoing.in_reply_to = Some(parsed.clone());
                outgoing.references.push(parsed);
            }
            Some(tid.clone())
        } else {
            let (_, recipient) = CONTACTS[d_idx % CONTACTS.len()];
            outgoing.to.push(Address {
                name: None,
                email: recipient.to_owned(),
            });
            outgoing.subject = Some(format!("Draft note #{}", d_idx + 1));
            None
        };
        if d_idx % 3 == 1 {
            // Rich-text HTML draft (exercises protected MIME handling).
            outgoing.text = Some("Draft reply with formatting.".to_owned());
            outgoing.html = Some(
                "<p><strong>Draft reply</strong> with <em>rich formatting</em>.</p>".to_owned(),
            );
        } else if d_idx % 3 == 2 {
            // Draft with BCC header.
            outgoing.bcc.push(Address {
                name: None,
                email: "archive@example.org".to_owned(),
            });
            outgoing.text = Some("Draft reply with BCC recipient.".to_owned());
        } else {
            outgoing.text = Some("Work-in-progress reply draft.".to_owned());
        }
        if let Ok(composed) = mailtide_mime::compose(
            &outgoing,
            &ComposeOptions {
                purpose: Purpose::Draft,
                max_bytes: None,
            },
        ) {
            account.create_draft(&composed.raw, thread, true);
            drafts_created += 1;
        }
    }

    SeedResult {
        threads_created: spec.threads,
        messages_created,
        drafts_created,
        labels_created: user_label_ids.len(),
        history_id: account.history_id,
    }
}

/// Summary returned after importing `.eml` files from a directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportEmlResult {
    /// Files successfully imported.
    pub imported: usize,
    /// Files skipped (unreadable or non-`.eml`).
    pub skipped: usize,
    /// Final `historyId`.
    pub history_id: u64,
}

/// Import all `*.eml` files from `dir` into `account`, grouping threads by `References` /
/// `In-Reply-To` / normalized `Subject`.
pub fn import_eml_dir(
    account: &mut AccountState,
    dir: &Path,
    default_labels: &[String],
) -> Result<ImportEmlResult, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("cannot read directory: {e}"))?;
    let mut paths = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("eml"))
        {
            paths.push(path);
        }
    }
    paths.sort();

    let mut msgid_to_thread: BTreeMap<String, String> = BTreeMap::new();
    let mut subject_to_thread: BTreeMap<String, String> = BTreeMap::new();
    let mut imported = 0_usize;
    let mut skipped = 0_usize;

    let labels = if default_labels.is_empty() {
        vec!["INBOX".to_owned()]
    } else {
        default_labels.to_vec()
    };

    for path in paths {
        let Ok(raw) = std::fs::read(&path) else {
            skipped += 1;
            continue;
        };
        let parsed = mailtide_mime::parse(&raw).ok();
        let header = |name: &str| -> Option<String> {
            parsed.as_ref().and_then(|p| {
                p.headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.clone())
            })
        };
        let irt = header("In-Reply-To");
        let refs = header("References").unwrap_or_default();
        let norm_subject = header("Subject")
            .map(|s| normalize_subject(&mailtide_mime::decode_header(&s)))
            .filter(|s| !s.is_empty());

        let existing_thread = irt
            .as_deref()
            .and_then(|id| msgid_to_thread.get(id.trim()).cloned())
            .or_else(|| {
                refs.split_whitespace()
                    .find_map(|id| msgid_to_thread.get(id.trim()).cloned())
            })
            .or_else(|| {
                norm_subject
                    .as_deref()
                    .and_then(|s| subject_to_thread.get(s).cloned())
            });

        let (stored, _) = account.add_message(None, existing_thread, labels.clone(), None, raw);
        if !stored.rfc822_message_id.is_empty() {
            msgid_to_thread.insert(stored.rfc822_message_id.clone(), stored.thread_id.clone());
        }
        if let Some(subj) = norm_subject {
            subject_to_thread.insert(subj, stored.thread_id.clone());
        }
        imported += 1;
    }

    Ok(ImportEmlResult {
        imported,
        skipped,
        history_id: account.history_id,
    })
}

fn normalize_subject(subject: &str) -> String {
    let mut s = subject.trim().to_ascii_lowercase();
    while let Some(rest) = s
        .strip_prefix("re:")
        .or_else(|| s.strip_prefix("fwd:"))
        .or_else(|| s.strip_prefix("fw:"))
    {
        s = rest.trim().to_owned();
    }
    s
}
