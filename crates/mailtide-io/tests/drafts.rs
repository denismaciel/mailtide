//! The draft calls against a fake Gmail: lifecycle, conflicts, lost
//! answers, drafts changed elsewhere (SPEC §7a).

pub mod fake_gmail;

use fake_gmail::*;

#[test]
fn a_draft_update_keeps_its_draft_id() {
    block_on(async {
        let (runner, _setup) = runner();
        synced(&runner).await;
        let first = runner
            .create_draft(&account(), raw(&token(1), "a"), None, BUDGET)
            .await
            .expect("create");
        let second = runner
            .update_draft(
                &account(),
                &first.id,
                raw(&token(1), "b"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect("update");
        assert_eq!(second.id, first.id);
        assert_ne!(second.message, first.message);
    });
}
