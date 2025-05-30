use std::sync::{Arc, MutexGuard};

use crate::{
    catalog::{
        info::{IndexInfo, TableInfo},
        Catalog,
    },
    exec::{
        expression::Evaluate,
        plan::{
            update::UpdatePlanNode,
            AbstractPlanNode,
        },
    },
    table::{
        page::TupleMeta,
        schema::{ColumnType, Schema},
        tuple::{Tuple, RID},
        value::{ColumnValue, IntegerValue},
    },
};

use super::{Execute, Executor};

pub struct UpdateExecutor {
    pub plan: UpdatePlanNode,
    pub catalog: Arc<Catalog>,
    pub child: Box<Executor>,
    updated: bool,
}

impl UpdateExecutor {
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

// TODO: potentially also use this in delete executor
/// Delete tuple with RID=`rid` from table with `table_info` and all indexes in `index_infos`.
fn delete_from_table_and_indexes(
    table_info: &MutexGuard<'_, TableInfo>,
    index_infos: &Vec<MutexGuard<'_, IndexInfo>>,
    rid: &RID,
) -> Tuple {
    let (mut meta, old_tuple) = table_info
        .table
        .get_tuple(rid)
        .expect(format!("Can't delete tuple that doesn't exist {:?}", rid).as_str());
    meta.is_deleted = true;
    table_info.table.update_tuple_meta(meta, rid);

    for index_info in index_infos.iter() {
        index_info.index.delete(&old_tuple, &table_info.schema);
    }

    old_tuple
}

// TODO: potentially use this in insert executor
/// Insert `new_tuple` in table with `table_info` and indexes in `index_infos`.
fn insert_tuple_in_table_and_indexes(
    table_info: &mut MutexGuard<'_, TableInfo>,
    index_infos: &Vec<MutexGuard<'_, IndexInfo>>,
    new_tuple: Tuple,
) {
    let new_rid = table_info
        .table
        .insert_tuple(
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
            new_tuple.clone(),
        )
        .expect("Couldn't insert updated ticket");

    for index_info in index_infos.iter() {
        index_info
            .index
            .insert(&new_tuple, &table_info.schema, new_rid.clone())
            .unwrap();
    }
}

// TODO: also potentially use this
fn int_tuple(value: i32) -> Tuple {
    Tuple::new(
        vec![ColumnValue::Integer(IntegerValue { value })],
        &Schema::with_types(vec![ColumnType::Integer]),
    )
}

impl Execute for UpdateExecutor {
    fn init(&mut self) {
        self.validate();

        self.child.init();
        self.updated = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.updated {
            return None;
        }

        let mut updated_tuples = 0;
        while let Some((_, rid)) = self.child.next() {
            let table_info = self.catalog.get_table_by_oid(self.plan.table_oid).expect(
                format!(
                    "Can't update table that doesn't exist ({} - {})",
                    self.plan.table_oid, self.plan.table_name
                )
                .as_str(),
            );
            let mut table_info = table_info.lock().unwrap();

            let index_infos = self.catalog.get_table_indexes(&self.plan.table_name);
            let index_infos = index_infos
                .iter()
                .map(|i| i.lock().unwrap())
                .collect::<Vec<_>>();

            let old_tuple = delete_from_table_and_indexes(&table_info, &index_infos, &rid);
            let new_tuple = self.get_updated_tuple(&old_tuple, &table_info);
            insert_tuple_in_table_and_indexes(&mut table_info, &index_infos, new_tuple);

            updated_tuples += 1;
        }

        Some((int_tuple(updated_tuples), RID::invalid()))
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        todo!()
    }
}
