package ee.audit.database;

import javax.sql.DataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class DatabaseApplication {

  public static void main(String[] args) {
    SpringApplication.run(DatabaseApplication.class, args);
  }
}
