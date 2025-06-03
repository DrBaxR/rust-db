use std::sync::{Arc, MutexGuard};

use crate::{
    catalog::{info::TableInfo, Catalog},
    exec::{
        expression::Evaluate,
        plan::{update::UpdatePlanNode, AbstractPlanNode},
    },
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

use super::{
    util::{delete_from_table_and_indexes, insert_tuple_in_table_and_indexes, int_tuple},
    Execute, Executor, ExecutorContext,
};

pub struct UpdateExecutor {
    pub plan: UpdatePlanNode,
    pub catalog: Arc<Catalog>,
    pub child: Box<Executor>,
    updated: bool,
    /// Used for keeping track of what tuples were already updated (deleted + inserted). It contains the new RIDs of
    /// all tuples that were already processed
    rids_processed: Vec<RID>,
}

impl UpdateExecutor {
    pub fn new(context: ExecutorContext, plan: UpdatePlanNode, child: Executor) -> Self {
        Self {
            plan,
            catalog: context.catalog,
            child: Box::new(child),
            updated: false,
            rids_processed: vec![],
        }
    }

    /// # Panics
    /// Will panic if expressions don't properly match table schema.
    fn validate(&self) {
        let table_info = self
            .catalog
            .get_table_by_oid(self.plan.table_oid)
            .expect("Invalid table for update executor");
        let table_info = table_info.lock().unwrap();

        assert_eq!(
            self.plan.expressions.len(),
            table_info.schema.get_cols_count(),
            "Update executor expressions count don't match schema columns count"
        );

        for i in 0..table_info.schema.get_cols_count() {
            assert_eq!(
                table_info.schema.get_col_type(i),
                *self.plan.expressions[i].return_type().col_type()
            );
        }
    }

    fn get_updated_tuple(&self, tuple: &Tuple, table_info: &MutexGuard<'_, TableInfo>) -> Tuple {
        let values = self
            .plan
            .expressions
            .iter()
            .map(|e| e.evaluate(&tuple, &table_info.schema))
            .collect::<Vec<_>>();

        Tuple::new(values, &table_info.schema)
    }
}

impl Execute for UpdateExecutor {
    fn init(&mut self) {
        self.validate();

        self.child.init();
        self.updated = false;
        self.rids_processed = vec![];
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.updated {
            return None;
        }

        let mut updated_tuples = 0;
        while let Some((_, rid)) = self.child.next() {
            if self.rids_processed.contains(&rid) {
                continue;
            }

            let (table_info, index_infos) = self
                .catalog
                .get_table_with_indexes(self.plan.table_oid, &self.plan.table_name);

            let mut table_info = table_info.lock().unwrap();
            let index_infos = index_infos
                .iter()
                .map(|i| i.lock().unwrap())
                .collect::<Vec<_>>();

            // update is done by deleting old tuple and inserting new tuple with update values of old tuple
            let old_tuple = delete_from_table_and_indexes(&table_info, &index_infos, &rid);
            let new_tuple = self.get_updated_tuple(&old_tuple, &table_info);
            let new_rid =
                insert_tuple_in_table_and_indexes(&mut table_info, &index_infos, new_tuple);

            updated_tuples += 1;
            self.rids_processed.push(new_rid);
        }

        self.updated = true;
        Some((int_tuple(updated_tuples), RID::invalid()))
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        let self_string = format!(
            "Update | Schema: {} | Table: {}({}) | Exprs: [ {} ]",
            self.output_schema().to_string(),
            self.plan.table_name,
            self.plan.table_oid,
            self.plan
                .expressions
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
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
