use super::{Column, ColumnType, Schema};

#[test]
fn schema_constructor_multi_cols() {
    let schema = Schema::new(vec![
        Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
        Column::new_fixed("small".to_string(), ColumnType::SmallInt),
        Column::new_fixed("bool".to_string(), ColumnType::Boolean),
    ]);

    // tuple structure: |.|..|.|
    assert_eq!(schema.offsets, vec![0, 1, 3]);
    assert_eq!(schema.tuple_length, 4);

    let schema = Schema::new(vec![
        Column::new_varchar("varchar".to_string(), 5),
        Column::new_fixed("timestamp".to_string(), ColumnType::Timestamp),
        Column::new_fixed("int".to_string(), ColumnType::Integer),
        Column::new_fixed("decimal".to_string(), ColumnType::Decimal),
        Column::new_fixed("timestamp".to_string(), ColumnType::Timestamp),
    ]);

    // tuple structure: |.....|........|....|........|........|
    assert_eq!(schema.offsets, vec![0, 5, 13, 17, 25]);
    assert_eq!(schema.tuple_length, 33);
}

#[test]
fn schema_constructor_one_col() {
    let schema = Schema::new(vec![Column::new_fixed(
        "int".to_string(),
        ColumnType::Integer,
    )]);

    assert_eq!(schema.offsets, vec![0]);
    assert_eq!(schema.tuple_length, 4);
}

#[test]
#[should_panic]
fn schema_constructor_no_cols() {
    let _ = Schema::new(vec![]);
}
