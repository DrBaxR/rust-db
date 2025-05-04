use std::sync::Arc;

use crate::{
    catalog::Catalog,
    exec::plan::{insert::InsertPlanNode, AbstractPlanNode},
    table::{
        page::TupleMeta,
        schema::Schema,
        tuple::{Tuple, RID},
        value::{ColumnValue, IntegerValue},
    },
};

use super::{Execute, Executor, ExecutorContext};

pub struct InsertExecutor {
    pub plan: InsertPlanNode,
    pub catalog: Arc<Catalog>,
    pub child: Box<Executor>,
    /// Whether the executor has already inserted the tuples or not.
    inserted: bool,
}

impl InsertExecutor {
    pub fn new(context: ExecutorContext, plan: InsertPlanNode, child: Executor) -> Self {
        Self {
            plan,
            catalog: context.catalog,
            child: Box::new(child),
            inserted: false,
        }
    }
}

impl Execute for InsertExecutor {
    fn init(&mut self) {
        self.inserted = false;
    }

    /// Inserts tuples into the table and returns the number of inserted tuples.
    ///
    /// # Assumption
    /// This executor assumes that the input tuple have the same schema as the table where we are inserting. This should
    /// be granted by the user of this executor (i.e. the planner).
    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.inserted {
            return None;
        }

        let table_oid = self.plan.table_oid;
        let table_info = self
            .catalog
            .get_table_by_oid(table_oid)
            .expect("Can't insert into a non-existing table");
        let mut table_info = table_info.lock().unwrap();
        let index_infos = self.catalog.get_table_indexes(&self.plan.table_name);

        let mut inserted_tuples = 0;
        while let Some((tuple, _)) = self.child.next() {
            // insert tuple into the table
            let tuple_rid = table_info
                .table
                .insert_tuple(
                    TupleMeta {
                        ts: 0,
                        is_deleted: false,
                    },
                    tuple.clone(),
                )
                .expect("Tuple too large to insert");

            // update indexes of table if they exist
            for index_info in &index_infos {
                let index_info = index_info.lock().unwrap();
                index_info
                    .index
                    .insert(&tuple, &self.child.output_schema(), tuple_rid.clone())
                    .unwrap();
            }

            inserted_tuples += 1;
        }

        self.inserted = true;
        Some((
            Tuple::new(
                vec![ColumnValue::Integer(IntegerValue {
                    value: inserted_tuples,
                })],
                &self.plan.get_output_schema(),
            ),
            RID::invalid(),
        ))
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        // TODO
        let self_string = format!(
            "Insert | Schema: {} | Table: {}({})",
            self.output_schema().to_string(),
            self.plan.table_name,
            self.plan.table_oid
        );

        let tabs = "\t".repeat(indent_level + 1);
        format!(
            "{}\n{}-> {}",
            self_string,
            tabs,
            self.child.to_string(indent_level + 1)
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        // TODO: write what is in the main function as a test
        // check that tuples were inserted into the table AND that the index was updated (also create a index before running the insert executor)
    }
}
