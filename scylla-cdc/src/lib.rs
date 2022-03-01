mod cdc_types;
pub mod consumer;
pub mod reader;
pub mod stream_generations;

#[cfg(test)]
mod tests {
    use crate::consumer::*;
    use async_trait::async_trait;
    use scylla::cql_to_rust::FromCqlVal;
    use scylla::frame::response::result::CqlValue;
    use scylla::{Session, SessionBuilder};
    use std::collections::{HashMap, VecDeque};
    use std::hash::Hash;
    use std::marker::{Send, Sync};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    async fn fail_test(mess: &str) {
        // [TODO]: Give more precise information.
        eprintln!("{}", mess);
    }

    // First one is vec of clustering keys, the second are other columns.
    // Only not null values should be present in these vectors.
    // If a value v is deleted, insert pair ("v", None) to the second vector.
    // [TODO]: Add support for non-frozen collections
    type Operation = (
        OperationType,
        Vec<(&'static str, CqlValue)>,
        Vec<(&'static str, Option<CqlValue>)>,
    );

    struct TestConsumer<T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> {
        read_operations: Arc<Mutex<HashMap<T, VecDeque<Operation>>>>,
        pk: String, // [TODO]: Add support for complex partition keys
    }

    impl<T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> TestConsumer<T> {
        fn new(
            read_operations: Arc<Mutex<HashMap<T, VecDeque<Operation>>>>,
            pk: String,
        ) -> TestConsumer<T> {
            TestConsumer {
                read_operations,
                pk,
            }
        }
    }

    #[async_trait]
    impl<T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> Consumer for TestConsumer<T> {
        async fn consume_cdc(&mut self, mut data: CDCRow<'_>) -> anyhow::Result<()> {
            let pk_val = T::from_cql(data.take_value(&self.pk).unwrap())?;
            let op = self
                .read_operations
                .lock()
                .await
                .get_mut(&pk_val)
                .unwrap()
                .pop_front()
                .unwrap();

            // Check if operation type is ok.
            if op.0 != data.operation {
                fail_test("Operation type not matching.").await;
            }

            // Check if clustering keys are ok.
            // [TODO]: Assure that data doesn't contain additional ck values
            let ck_ok =
                op.1.iter()
                    .map(|(name, value)| {
                        *value
                            == *data
                                .get_value(name)
                                .as_ref()
                                .expect(&format!("Column {} not found", name))
                    })
                    .fold(true, |acc, x| acc && x);
            if !ck_ok {
                fail_test("Clustering keys not matching.").await;
            }

            // Check if values are ok.
            // [TODO]: Assure that data doesn't change other columns
            let values_ok =
                op.2.iter()
                    .map(|(name, value)| match value {
                        Some(val) => *val == *data.get_value(name).as_ref().unwrap(),
                        None => data.is_value_deleted(name),
                    })
                    .fold(true, |acc, x| acc && x);
            if !values_ok {
                fail_test("Values not matching.").await;
            }
            Ok(())
        }
    }

    struct TestConsumerFactory<T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> {
        read_operations: Arc<Mutex<HashMap<T, VecDeque<Operation>>>>,
        pk: String,
    }

    impl<T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> TestConsumerFactory<T> {
        fn new(pk: String, operations: HashMap<T, VecDeque<Operation>>) -> TestConsumerFactory<T> {
            TestConsumerFactory {
                read_operations: Arc::new(Mutex::new(operations)),
                pk,
            }
        }
    }

    #[async_trait]
    impl<T: 'static + FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> ConsumerFactory
        for TestConsumerFactory<T>
    {
        async fn new_consumer(&self) -> Box<dyn Consumer> {
            Box::new(TestConsumer {
                read_operations: Arc::clone(&self.read_operations),
                pk: self.pk.clone(),
            })
        }
    }

    fn get_uri() -> String {
        std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string())
    }

    async fn get_session() -> anyhow::Result<Session> {
        Ok(SessionBuilder::new().known_node(get_uri()).build().await?)
    }

    // Creates test table and keyspace. Query should not contain keyspace name, only the table name.
    async fn create_table_and_keyspace(
        session: &Session,
        table_name: &str,
        create_query: &str,
    ) -> anyhow::Result<()> {
        session.query("CREATE KEYSPACE IF NOT EXISTS e2e-test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ()).await?;
        session.use_keyspace("e2e-test", false).await?;

        session.query(format!("DROP TABLE IF EXISTS {}", table_name), ()).await?;
        session.query(create_query, ()).await?;

        Ok(())
    }

    struct Test<'a, T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> {
        session: Session,
        performed_operations: HashMap<T, VecDeque<Operation>>,
        table_name: &'a str,

        // Queries.
        insert_query: &'a str,
        update_query: &'a str,
    }

    impl<'a, T: FromCqlVal<CqlValue> + Eq + Hash + Send + Sync> Test<'a, T> {
        async fn new(
            session: Session,
            table_name: &'a str,
            create_query: &str,
            insert_query: &'a str,
            update_query: &'a str,
        ) -> anyhow::Result<Test<'a, T>> {
            create_table_and_keyspace(&session, table_name, create_query).await?;

            Ok(Test {
                session,
                performed_operations: HashMap::new(),
                table_name,
                insert_query,
                update_query,
            })
        }

        async fn insert(&mut self, pk: T, values_to_bind: &[Option(CqlValue)]) -> anyhow::Result<()> {
            self.session.query(self.insert_query, values_to_bind).await?;
            self.performed_operations.entry(&pk).or_insert(VecDeque::new()).push_back(operation);

            Ok(())
        }
    }

    #[tokio::test]
    async fn sample_test() {
        let session = get_session().await.unwrap();
        let mut test = Test::<i32>::new(
            session,
            "int_test",
            "CREATE TABLE int_test (pk int, ck int, v int, primary key (pk, ck)) WITH cdc = {{'enabled' : true}}",
            "INSERT INTO int_test (pk, ck, v) VALUES (?, ?, ?)",
            "UPDATE int_test SET v = ? WHERE pk = ? AND ck = ?",
        ).await.unwrap();


    }
}
