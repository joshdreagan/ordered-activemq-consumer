package org.apache.camel.processor.aggregate.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.spi.AggregationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.SqlLobValue;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

public class OptimisticLockingJdbcAggregationRepository implements AggregationRepository {

  private static final Logger log = LoggerFactory.getLogger(OptimisticLockingJdbcAggregationRepository.class);

  protected static final String ID_KEY = "id";
  protected static final String EXCHANGE_KEY = "exchange";
  protected static final String BODY_KEY = "body";
  protected static final String VERSION_KEY = "version";

  public static final String VERSION_EXCHANGE_PROPERTY = "CamelOptimisticLockVersion";

  private final PlatformTransactionManager transactionManager;
  private final String repositoryName;
  private final DataSource dataSource;

  private String repositoryNameCompleted;
  private JdbcCamelCodec codec;
  private LobHandler lobHandler;

  private TransactionTemplate transactionTemplate;
  private JdbcTemplate jdbcTemplate;

  public OptimisticLockingJdbcAggregationRepository(PlatformTransactionManager transactionManager, String repositoryName, DataSource dataSource) {
    this.transactionManager = transactionManager;
    this.repositoryName = repositoryName;
    this.dataSource = dataSource;
  }

  public PlatformTransactionManager getTransactionManager() {
    return transactionManager;
  }

  public String getRepositoryName() {
    return repositoryName;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public String getRepositoryNameCompleted() {
    if (repositoryNameCompleted == null) {
      repositoryNameCompleted = getRepositoryName() + "_completed";
    }
    return repositoryNameCompleted;
  }

  public void setRepositoryNameCompleted(String repositoryNameCompleted) {
    this.repositoryNameCompleted = repositoryNameCompleted;
  }

  public JdbcCamelCodec getCodec() {
    if (codec == null) {
      this.codec = new JdbcCamelCodec();
    }
    return codec;
  }

  public void setCodec(JdbcCamelCodec codec) {
    this.codec = codec;
  }

  public LobHandler getLobHandler() {
    if (lobHandler == null) {
      this.lobHandler = new DefaultLobHandler();
    }
    return lobHandler;
  }

  public void setLobHandler(LobHandler lobHandler) {
    this.lobHandler = lobHandler;
  }

  protected TransactionTemplate getTransactionTemplate() {
    if (transactionTemplate == null) {
      transactionTemplate = new TransactionTemplate(transactionManager);
      transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    }
    return transactionTemplate;
  }

  protected JdbcTemplate getJdbcTemplate() {
    if (jdbcTemplate == null) {
      jdbcTemplate = new JdbcTemplate(dataSource);
    }
    return jdbcTemplate;
  }

  @Override
  public Exchange add(CamelContext camelContext, String key, Exchange exchange) {
    return getTransactionTemplate().execute(new TransactionCallback<Exchange>() {

      @Override
      public Exchange doInTransaction(TransactionStatus status) {
        Exchange result = get(camelContext, key);
        try {
          log.debug(String.format("Adding exchange with key: [%s]", key));

          byte[] marshalledExchange = getCodec().marshallExchange(camelContext, exchange, true);

          boolean present = getJdbcTemplate().queryForObject(
                  String.format("SELECT COUNT(*) FROM %1$s WHERE %2$s=?", getRepositoryName(), ID_KEY), 
                  new Object[]{key},
                  new int[]{Types.VARCHAR}, 
                  Integer.class) != 0;
          if (present) {
            long version = exchange.getProperty(VERSION_EXCHANGE_PROPERTY, Long.class);
            log.debug(String.format("Updating record with key: [%s] and version: [%s].", key, version));
            int affectedRows = getJdbcTemplate().update(
                    String.format("UPDATE %1$s SET %2$s=?, %3$s=? WHERE %4$s=? AND %3$s=?", getRepositoryName(), EXCHANGE_KEY, VERSION_KEY, ID_KEY),
                    new Object[]{new SqlLobValue(marshalledExchange, getLobHandler()), version + 1, key, version},
                    new int[]{Types.BLOB, Types.BIGINT, Types.VARCHAR, Types.BIGINT});
            if (affectedRows < 1) {
              throw new RuntimeException(String.format("Error updating record with key: [%s] and version: [%s]. Stale version...", key, version));
            }
          } else {
            log.debug(String.format("Inserting record with key: [%s].", key));
            getJdbcTemplate().update(
                    String.format("INSERT INTO %1$s (%2$s, %3$s, %4$s) VALUES (?, ?, ?)", getRepositoryName(), ID_KEY, EXCHANGE_KEY, VERSION_KEY),
                    new Object[]{key, new SqlLobValue(marshalledExchange, getLobHandler()), 1L},
                    new int[]{Types.VARCHAR, Types.BLOB, Types.BIGINT});
          }
        } catch (Exception e) {
          throw new RuntimeException(String.format("Error adding to repository [%s] with key [%s].", getRepositoryName(), key), e);
        }

        return result;
      }
    });
  }

  @Override
  public Exchange get(CamelContext camelContext, String key) {
    return getTransactionTemplate().execute(new TransactionCallback<Exchange>() {

      @Override
      public Exchange doInTransaction(TransactionStatus status) {
        try {
          Map<String, Object> columns = getJdbcTemplate().queryForMap(
                  String.format("SELECT %1$s, %2$s FROM %3$s WHERE %4$s=?", EXCHANGE_KEY, VERSION_KEY, getRepositoryName(), ID_KEY), 
                  new Object[]{key},
                  new int[]{Types.VARCHAR});
          byte[] marshalledExchange = (byte[]) columns.get(EXCHANGE_KEY);
          long version = (long) columns.get(VERSION_KEY);

          Exchange result = getCodec().unmarshallExchange(camelContext, marshalledExchange);
          result.setProperty(VERSION_EXCHANGE_PROPERTY, version);
          return result;
        } catch (EmptyResultDataAccessException e) {
          return null;
        } catch (IOException e) {
          throw new RuntimeException(String.format("Error getting key [%s] from repository [%s].", key, getRepositoryName()), e);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Override
  public void remove(CamelContext camelContext, String key, Exchange exchange) {
    getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {

      @Override
      protected void doInTransactionWithoutResult(TransactionStatus status) {
        try {
          log.debug(String.format("Removing key [%s]", key));

          String confirmKey = exchange.getExchangeId();
          long version = exchange.getProperty(VERSION_EXCHANGE_PROPERTY, Long.class);
          byte[] marshalledExchange = getCodec().marshallExchange(camelContext, exchange, true);

          getJdbcTemplate().update(
                  String.format("DELETE FROM %1$s WHERE %2$s = ? AND %3$s = ?", getRepositoryName(), ID_KEY, VERSION_KEY),
                  new Object[]{key, version},
                  new int[]{Types.VARCHAR, Types.BIGINT});

          getJdbcTemplate().update(
                  String.format("INSERT INTO %1$s (%2$s, %3$s, %4$s) VALUES (?, ?, ?)", getRepositoryNameCompleted(), ID_KEY, EXCHANGE_KEY, VERSION_KEY),
                  new Object[]{confirmKey, new SqlLobValue(marshalledExchange, getLobHandler()), 1L},
                  new int[]{Types.VARCHAR, Types.BLOB, Types.BIGINT});
        } catch (Exception e) {
          throw new RuntimeException(String.format("Error removing key [%s] from repository [%s]", key, getRepositoryName()), e);
        }
      }
    });
  }

  @Override
  public void confirm(CamelContext camelContext, String exchangeId) {
    getTransactionTemplate().execute(new TransactionCallbackWithoutResult() {

      @Override
      protected void doInTransactionWithoutResult(TransactionStatus status) {
        log.debug(String.format("Confirming exchangeId [%s]", exchangeId));

        getJdbcTemplate().update(
                String.format("DELETE FROM %1$s WHERE %2$s=?", getRepositoryNameCompleted(), ID_KEY), exchangeId);
      }
    });
  }

  @Override
  public Set<String> getKeys() {
    return getTransactionTemplate().execute(new TransactionCallback<LinkedHashSet<String>>() {

      @Override
      public LinkedHashSet<String> doInTransaction(TransactionStatus status) {
        List<String> keys = getJdbcTemplate().query(
                String.format("SELECT %s FROM %s", ID_KEY, getRepositoryName()), new RowMapper<String>() {

          @Override
          public String mapRow(ResultSet rs, int rowNum) throws SQLException {
            String id = rs.getString(ID_KEY);
            log.trace(String.format("getKey [%s]", id));
            return id;
          }
        });

        return new LinkedHashSet<>(keys);
      }
    });
  }
}
