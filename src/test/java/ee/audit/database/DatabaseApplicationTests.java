package ee.audit.database;

import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.ibatis.io.Resources;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
class DatabaseApplicationTests {

  @Autowired
  JdbcTemplate jdbcTemplate;

//  @Test
  void timeInsertsNew() {
    runScript("current_audit_fields_scripts.sql");
    jdbcTemplate.execute("select loging_test_schema.create_audit_triggers('loging_test_schema', 'person');");

    long totalTime = totalTimeOfInsertions();
    System.out.println("Total time to insert persons: " + totalTime + "ms");

    createTablesAndTriggers();

    totalTime = totalTimeOfInsertions();
    System.out.println("Total time to insert persons with auditing: " + totalTime + "ms");
  }

  @Test
  void timeInsertsNewWithOptimizations() {

    runScript("create_person_table.sql");
    runScript("create_or_update_trigger.sql");
    jdbcTemplate.execute("select create_or_update('loging_test_schema', 'person');");

    long totalTime = totalTimeOfInsertions();
    System.out.println("Total time to insert persons with auditing: " + totalTime + "ms");
  }


  @Test
  void shouldAddNewColumnToAuditTableWhenCallingCreateAndUpdateAgain() {
    createTablesAndTriggers();
    String name = randomString();
    double amount = 0.434;

    jdbcTemplate.execute(format("ALTER TABLE loging_test_schema.person ADD COLUMN amount NUMERIC(10, 2) NOT NULL;"));
    jdbcTemplate.execute("select create_or_update('loging_test_schema', 'person');");
    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name, amount) values ('%s', %s);", name, amount));

    Double result = jdbcTemplate.queryForObject("SELECT amount FROM loging_test_schema_log.person_log WHERE name=?;", new Object[]{name}, Double.class);
    assertThat(result).isEqualTo(0.43);
  }

  @Test
  void shouldHaveOperationColumnAsInsertWhenInserting() {
    createTablesAndTriggers();
    String name = randomString();

    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name) values ('%s');", name));
    Map<String, String> result = jdbcTemplate.query(format("SELECT * FROM loging_test_schema_log.person_log WHERE name='%s';", name),
        (ResultSet rs) -> {
          rs.next();
          return Map.of("name", rs.getString("name"),
              "operation", rs.getString("operation"));
        }
    );

    assertThat(name).isEqualTo(result.get("name"));
    assertThat(result.get("operation")).isEqualTo("INSERT");
  }

  @Test
  void shouldHaveOperationColumnUpdateWhenUpdating() {
    createTablesAndTriggers();
    String name = randomString();
    String name_new = name + "_new";

    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name) values ('%s');", name));
    jdbcTemplate.execute(format("UPDATE loging_test_schema.person SET name='%s' WHERE name='%s';", name_new, name));
    Map<String, String> result = jdbcTemplate.query(format("SELECT * FROM loging_test_schema_log.person_log WHERE name='%s';", name_new),
        (ResultSet rs) -> {
          rs.next();
          return Map.of("name", rs.getString("name"),
              "operation", rs.getString("operation"));
        }
    );

    assertThat(name_new).isEqualTo(result.get("name"));
    assertThat("UPDATE").isEqualTo(result.get("operation"));
  }

  @Test
  void shouldWriteToAuditWhenDelete() {
    createTablesAndTriggers();
    String name = randomString();

    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name) values ('%s');", name));
    jdbcTemplate.execute(format("DELETE FROM loging_test_schema.person where name='%s';", name));

    List<String> result = jdbcTemplate.queryForList("SELECT operation FROM loging_test_schema_log.person_log WHERE name=? ORDER BY log_id;",
        new Object[]{name}, String.class);

    assertThat(result).containsExactly("INSERT", "DELETE");
  }

  @Test
  void shouldLogTheDeletedRowToLogWhenDeleting() {
    createTablesAndTriggers();
    String name = randomString();

    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name) values ('%s');", name));
    jdbcTemplate.execute(format("DELETE FROM loging_test_schema.person where name='%s';", name));

    String result = jdbcTemplate.queryForObject("SELECT name FROM loging_test_schema_log.person_log WHERE operation='DELETE' ORDER BY log_id;"
        , String.class);

    assertThat(result).isEqualTo(name);
  }


  @Test
  void shouldCreateSeparateTriggerProceduresForSeparateTables() {
    createTablesAndTriggers();
    createTransactionTable();
    String name = randomString();
    String account = randomString();
    Double amount = 0.65;

    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name) values ('%s');", name));
    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.transaction (account, amount) values ('%s', %s);", account, amount));

    // Should not produce exception.
  }

  @Test
  void shouldThrowErrorWhenNoAuditFieldsPresentWhenInserting() {
    createTablesAndTriggers();
    createTransactionTable();
    String name = randomString();
    String account = randomString();
    Double amount = 0.65;

    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.person (name) values ('%s');", name));
    jdbcTemplate.execute(format("INSERT INTO loging_test_schema.transaction (account, amount) values ('%s', %s);", account, amount));
  }

  @Test
  void shouldHaveLogCreatedByAndLogCreatedAtFields() {

  }


  @Test
  void shouldModifiedCreatedByAndAtColumns() {

  }

  @Test
  void shouldCreateIndexOnPrimaryKeyColumnInLogTableAsWell() {
    // Don't know why index on log table is needed actually.
  }

  @Test
  void shouldNotAllowUpdatingLogTable() {

  }

  @Test
  void shouldAddNumbericColumnWithPercision() {
//    Isn't needed because it takes the value from new. And NEW already has the truncated value.
//    fail("Should we add numeric variables to audit table as well?");
  }

  @Test
  void shouldAlterTableTypeWhenBaseTableTypeAltered() {
    fail("Should We Alter Table Type When Base Table Type Altered?");
  }

  @Test
  void shouldWeAlterTableNameWhenColumnDeleted() {
    fail("should we alter table name, when column deleted?");
  }

  private void createTablesAndTriggers() {
    runScript("create_person_table.sql");
    runScript("create_or_update_trigger.sql");
    jdbcTemplate.execute("select create_or_update('loging_test_schema', 'person');");
  }

  private void createTransactionTable() {
    runScript("create_transaction_table.sql");
    jdbcTemplate.execute("select create_or_update('loging_test_schema', 'transaction');");
  }

  private String randomString() {
    return String.valueOf(new Random().nextDouble());
  }

  private long totalTimeOfInsertions() {
    BufferedReader reader = new BufferedReader(getResourceAsReader("n_number_of_inserts_to_person_table.sql"));
    String scriptAsString = reader.lines().collect(Collectors.joining(" "));
    LocalDateTime start = LocalDateTime.now();
    jdbcTemplate.execute(scriptAsString);
    return LocalDateTime.now().toInstant(UTC).toEpochMilli() - start.toInstant(UTC).toEpochMilli();
  }

  private void runScript(String s) {
    BufferedReader reader = new BufferedReader(getResourceAsReader(s));
    jdbcTemplate.execute(reader.lines().collect(Collectors.joining("\n")));
  }

  private Reader getResourceAsReader(String s) {
    try {
      return Resources.getResourceAsReader(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
