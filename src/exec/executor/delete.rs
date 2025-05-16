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
        let table_info = self
            .catalog
            .get_table_by_oid(table_oid)
            .expect("Can't insert into a non-existing table");
        let table_info = table_info.lock().unwrap();

        let index_infos = self.catalog.get_table_indexes(&self.plan.table_name);
        let index_infos = index_infos
            .iter()
            .map(|i| i.lock().unwrap())
            .collect::<Vec<_>>();

        let mut deleted_tuples = 0;
        while let Some((_, rid)) = self.child.next() {
            let (mut meta, tuple) = table_info
                .table
                .get_tuple(&rid)
                .expect(format!("Can't delete a non-existing tuple with RID={:?}", rid).as_str());

            meta.is_deleted = true;
            table_info.table.update_tuple_meta(meta, &rid);

            for index_info in index_infos.iter() {
                index_info.index.delete(&tuple, self.child.output_schema());
            }

            deleted_tuples += 1;
        }

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
        todo!()
    }
}

// TODO: test this
