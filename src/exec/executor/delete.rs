use std::sync::Arc;

use crate::{
    catalog::Catalog,
    exec::plan::{delete::DeletePlanNode, AbstractPlanNode},
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
        value::{ColumnValue, IntegerValue},
    },
};

use super::{Execute, Executor, ExecutorContext};

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

        let table_oid = self.plan.table_oid;

        let mut deleted_tuples = 0;
        while let Some((_, rid)) = self.child.next() {
            // table info lock needs to be acquired here to prevent deadlock 
            // (i.e. if we hold it for too long the case where we have something like DeleteExecutor -> SeqScanExecutor, we would have a deadlock
            // since the SeqScanExecutor would try to acquire the table lock that the DeleteExecutor already holds)
            let table_info = self
                .catalog
                .get_table_by_oid(table_oid)
                .expect("Can't insert into a non-existing table");
            let table_info = table_info.lock().unwrap();

            let (mut meta, tuple) = table_info
                .table
                .get_tuple(&rid)
                .expect(format!("Can't delete a non-existing tuple with RID={:?}", rid).as_str());

            meta.is_deleted = true;
            table_info.table.update_tuple_meta(meta, &rid);

            let index_infos = self.catalog.get_table_indexes(&self.plan.table_name);
            let index_infos = index_infos
                .iter()
                .map(|i| i.lock().unwrap())
                .collect::<Vec<_>>();

            for index_info in index_infos.iter() {
                index_info.index.delete(&tuple, self.child.output_schema());
            }

            deleted_tuples += 1;
        }

        self.deleted = true;
        Some((
            Tuple::new(
                vec![ColumnValue::Integer(IntegerValue {
                    value: deleted_tuples,
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

// TODO: test this
