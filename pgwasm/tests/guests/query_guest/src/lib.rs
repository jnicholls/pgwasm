wit_bindgen::generate!({
    world: "guest",
    path: "wit",
    generate_all,
});

use crate::pgwasm::host::query;

struct QueryGuest;

impl Guest for QueryGuest {
    fn run() -> String {
        let sql = "SELECT 1 AS a, 'x' AS b";
        match query::read(sql, &[]) {
            Ok(rs) => {
                let mut out = String::new();
                out.push_str(&format!("cols={}", rs.column_names.len()));
                if let Some(row) = rs.rows.first() {
                    out.push_str(&format!(",cells={}", row.columns.len()));
                }
                out
            }
            Err(e) => format!("ERR:{e}"),
        }
    }
}

export!(QueryGuest);
