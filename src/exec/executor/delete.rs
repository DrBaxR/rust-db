use std::sync::Arc;

use crate::{
    catalog::Catalog,
    exec::plan::{delete::DeletePlanNode, AbstractPlanNode},
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

use super::{
    util::{delete_from_table_and_indexes, int_tuple},
    Execute, Executor, ExecutorContext,
};

pub struct DeleteExecutor {
    pub plan: DeletePlanNode,
    pub catalog: Arc<Catalog>,
    pub child: Box<Executor>,
    /// Whether the executor has already deleted the tuples or not.
    deleted: bool,
}

impl DeleteExecutor {
    pub fn new(context: ExecutorContext, plan: DeletePlanNode, child: Executor) -> Self {
        Self {
            plan,
            catalog: context.catalog,
            child: Box::new(child),
            deleted: false,
        }
    }
}

impl Execute for DeleteExecutor {
    fn init(&mut self) {
        self.child.init();
        self.deleted = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.deleted {
            return None;
        }

        let mut deleted_tuples = 0;
        while let Some((_, rid)) = self.child.next() {
            // table info lock needs to be acquired here to prevent deadlock
            // (i.e. if we hold it for too long the case where we have something like DeleteExecutor -> SeqScanExecutor, we would have a deadlock
            // since the SeqScanExecutor would try to acquire the table lock that the DeleteExecutor already holds)
            let (table_info, index_infos) = self
                .catalog
                .get_table_with_indexes(self.plan.table_oid, &self.plan.table_name);

            let table_info = table_info.lock().unwrap();
            let index_infos = index_infos
                .iter()
                .map(|i| i.lock().unwrap())
                .collect::<Vec<_>>();

            delete_from_table_and_indexes(&table_info, &index_infos, &rid);

            deleted_tuples += 1;
        }

        self.deleted = true;
        Some((int_tuple(deleted_tuples), RID::invalid()))
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        let self_string = format!(
            "Delete | Schema: {} | Table: {}({})",
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
    use std::{env::temp_dir, fs::remove_file};

    use crate::{
        exec::{
            executor::{Execute, Executor},
            plan::PlanNode,
        },
        sample_code::executors::{delete_executor, seq_scan_executor, TableConstructorType},
        table::{
            schema::{ColumnType, Schema},
            tuple::Tuple,
        },
        test_utils::decimal_value,
    };

    #[test]
    fn run_delete_executor() {
        // init
        let db_path = temp_dir().join("delete_run_delete_executor.db");
        let (scan_executor, table_context) = seq_scan_executor(TableConstructorType::WithTable(
            db_path.to_str().unwrap().to_string(),
        ));
        let table_name = scan_executor.plan.table_name.clone();
        let tuples_schema = table_context.1.clone();

        let (mut delete_executor, schema) = delete_executor(
            PlanNode::SeqScan(scan_executor.plan.clone()),
            Executor::SeqScan(scan_executor),
            TableConstructorType::WithoutTable(table_context.clone()),
        );

        // create index for table
        let index_name = "index";
        let key_schema = Schema::with_types(vec![ColumnType::Integer]);
        let key_size = key_schema.get_tuple_len();
        table_context
            .0
            .catalog
            .create_index(
                index_name,
                &delete_executor.plan.table_name,
                tuples_schema.clone(),
                key_schema.clone(),
                vec![0],
                key_size,
            )
            .unwrap();

        // check initial state of table
        let (mut tmp_scan_executor, _) =
            seq_scan_executor(TableConstructorType::WithoutTable(table_context.clone()));

        tmp_scan_executor.init();
        let mut initial_tuples_count = 0;
        let mut initial_tuples = vec![];
        while let Some((tuple, _)) = tmp_scan_executor.next() {
            initial_tuples_count += 1;
            initial_tuples.push(tuple);
        }

        // check initial state of index
        let index_info = table_context
            .0
            .catalog
            .get_index_by_name(index_name, &table_name)
            .unwrap();
        let index_info = index_info.lock().unwrap();
        for tuple in initial_tuples.iter() {
            let key = Tuple::from_projection(&tuple, &tuples_schema, &key_schema, &vec![0]);

            let rids = index_info.index.scan(key);
            assert_eq!(rids.len(), 1);
        }
        drop(index_info);

        assert_eq!(initial_tuples_count, 3);

        // run delete executor
        delete_executor.init();
        while let Some((tuple, _)) = delete_executor.next() {
            assert_eq!(
                tuple.get_value(&schema, 0).to_decimal().unwrap(),
                decimal_value(3.)
            );
        }

        // check final state of table
        let (mut tmp_scan_executor, _) =
            seq_scan_executor(TableConstructorType::WithoutTable(table_context.clone()));

        tmp_scan_executor.init();
        let mut final_tuples = 0;
        while let Some((_, _)) = tmp_scan_executor.next() {
            final_tuples += 1;
        }

        assert_eq!(final_tuples, 0);

        // check final state of index
        let index_info = table_context
            .0
            .catalog
            .get_index_by_name(index_name, &table_name)
            .unwrap();
        let index_info = index_info.lock().unwrap();
        for tuple in initial_tuples.iter() {
            let key = Tuple::from_projection(&tuple, &tuples_schema, &key_schema, &vec![0]);

            let rids = index_info.index.scan(key);
            assert_eq!(rids.len(), 0);
        }
        drop(index_info);

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }
}
