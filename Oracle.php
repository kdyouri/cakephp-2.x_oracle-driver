<?php
/**
 * Oracle layer for DBO
 *
 * CakePHP(tm) : Rapid Development Framework (http://cakephp.org)
 */

App::uses('DboSource', 'Model/Datasource');

/**
 * Dbo layer for OCI driver
 *
 * A Dbo layer for OCI. Requires the
 * `pdo_oci` extension to be enabled.
 *
 * @link http://www.php.net/manual/en/ref.pdo-oci.php
 *
 * @package       Cake.Model.Datasource.Database
 */
class Oracle extends DboSource {

/**
 * Driver description
 *
 * @var string
 */
	public $description = "Oracle DBO Driver";

/**
 * Starting quote character for quoted identifiers
 *
 * @var string
 */
	public $startQuote = '"';

/**
 * Ending quote character for quoted identifiers
 *
 * @var string
 */
	public $endQuote = '"';

/**
 * Creates a map between field aliases and numeric indexes. Workaround for the
 * Oracle driver's 30-character column name limitation.
 *
 * @var array
 */
	protected $_fieldMappings = array();

/**
 * Storing the last affected value
 *
 * @var mixed
 */
	protected $_lastAffected = false;

/**
 * Database keyword used to assign aliases to identifiers.
 *
 * @var string
 */
	public $alias = ' ';

/**
 * Base configuration settings for Oracle driver
 *
 * @var array
 */
	protected $_baseConfig = array(
		'persistent' => true,
		'login' => '',
		'password' => '',
		'database' => 'localhost/XE',
		'encoding' => 'utf8',
	);

/**
 * Oracle column definition
 *
 * @var array
 */
	public $columns = array(
		'primary_key' => array('name' => ''),
		'string' => array('name' => 'varchar2', 'limit' => '255'),
		'text' => array('name' => 'varchar2'),
		'integer' => array('name' => 'number'),
		'float' => array('name' => 'float'),
		'datetime' => array('name' => 'date', 'format' => 'Y-m-d H:i:s'),
		'timestamp' => array('name' => 'date', 'format' => 'Y-m-d H:i:s'),
		'time' => array('name' => 'date', 'format' => 'Y-m-d H:i:s'),
		'date' => array('name' => 'date', 'format' => 'Y-m-d H:i:s'),
		'binary' => array('name' => 'bytea'),
		'boolean' => array('name' => 'boolean'),
		'number' => array('name' => 'number'),
		'inet' => array('name' => 'inet'),
	);

/**
 * Magic column name used to provide pagination support for Oracle
 * which lacks proper limit/offset support.
 *
 * @var string
 */
	const ROW_COUNTER = 'cake_page_rownum';

/**
 * Connects to the database using options in the given configuration array.
 *
 * @return boolean True if the database could be connected, else false
 * @throws MissingConnectionException
 */
	public function connect() {
		$config = $this->config;
		$this->connected = false;

		$flags = array(
			PDO::ATTR_PERSISTENT => $config['persistent'],
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_ORACLE_NULLS => true,
			PDO::NULL_EMPTY_STRING => true,
		);

		$charset = '';
		if (!empty($config['encoding'])) {
			$charset = '; charset=' . $config['encoding'];
		}

		try {
			$this->_connection = new PDO(
				"oci:dbname={$config['database']}{$charset}",
				$config['login'],
				$config['password'],
				$flags
			);

			$sql = "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS'";
			$this->_connection->exec($sql);

			$this->connected = true;
		} catch (PDOException $e) {
			throw new MissingConnectionException(array(
				'class' => get_class($this),
				'message' => $e->getMessage()
			));
		}

		return $this->connected;
	}

/**
 * Check that PDO Oracle is installed/loaded
 *
 * @return boolean
 */
	public function enabled() {
		return in_array('oci', PDO::getAvailableDrivers());
	}

/**
 * Returns an array of sources (tables) in the database.
 *
 * @param mixed $data
 * @return array Array of table names in the database
 */
	public function listSources($data = null) {
		$cache = parent::listSources();
		if ($cache !== null) {
			return $cache;
		}
		$result = $this->_execute("SELECT view_name name FROM user_views UNION SELECT table_name name FROM user_tables");

		if (!$result) {
			$result->closeCursor();
			return array();
		}
		$tables = array();

		while ($line = $result->fetch(PDO::FETCH_NUM)) {
			$tables[] = strtolower($line[0]);
		}

		$result->closeCursor();
		parent::listSources($tables);
		return $tables;
	}

/**
 * Returns an array of the fields in given table name.
 *
 * @param Model|string $model Model object to describe, or a string table name.
 * @return array Fields in table. Keys are name and type
 * @throws CakeException
 */
	public function describe($model) {
		$table = $this->fullTableName($model, false);
		$cache = parent::describe($table);
		if ($cache) {
			return $cache;
		}
		$fields = array();
		$cols = $this->_execute(
			'SELECT 
				COLUMN_NAME as "Field", 
				DATA_TYPE as "Type", 
				DATA_LENGTH as "Length",
				NULLABLE as "Null",
				DATA_DEFAULT as "Default",
				DATA_PRECISION as "Precision",
				DATA_SCALE as "Size"
			FROM user_tab_columns 
			WHERE table_name = \'' . strtoupper($table) . '\''
		);
		if (!$cols) {
			throw new CakeException(__d('cake_dev', 'Could not describe table for %s', $table));
		}
		$primaryKey = $this->_execute(
			'SELECT column_name
			FROM user_constraints
			NATURAL JOIN user_cons_columns
			WHERE constraint_type=\'P\'
				AND status=\'ENABLED\'
				AND validated=\'VALIDATED\'
				AND table_name=\'' . strtoupper($table) . '\''
		);
		if ($key = $primaryKey->fetch(PDO::FETCH_NUM)) $key = $key[0];

		while ($column = $cols->fetch(PDO::FETCH_OBJ)) {
			$field = strtolower($column->Field);
			$fields[$field] = array(
				'type' => $this->column($column),
				'null' => ($column->Null === 'Y' ? true : false),
				'default' => $column->Default,
				'length' => $this->length($column),
				'key' => ($key == $column->Field) ? 'primary' : false
			);

			if ($fields[$field]['default'] === 'null' || $fields[$field]['default'] === 'NULL') {
				$fields[$field]['default'] = null;
			}
			if ($fields[$field]['default'] !== null) {
				$fields[$field]['default'] = preg_replace("/^'?(.*)'/", "$1", $fields[$field]['default']);
				$this->value($fields[$field]['default'], $fields[$field]['type']);
			}
			if ($fields[$field]['key'] === false) {
				unset($fields[$field]['key']);
			}
		}
		$this->_cacheDescription($table, $fields);
		$cols->closeCursor();
		return $fields;
	}

/**
 * This method should quote Oracle identifiers. Well it doesn't.
 * It would break all scaffolding and all of Cake's default assumptions.
 *
 * @param unknown_type $var
 * @return unknown
 * @access public
 */
	function name($name) {
		if (strpos($name, '.') !== false && strpos($name, '"') === false) {
			list($model, $field) = explode('.', $name);
			if ($field[0] == "_") {
				$name = "$model.\"$field\"";
			}
		} else {
			if ($name[0] == "_") {
				$name = "\"$name\"";
			}
		}
		return $name;
	}
/**
 * Generates the fields list of an SQL query.
 *
 * @param Model $model
 * @param string $alias Alias table name
 * @param array $fields
 * @param boolean $quote
 * @return array
 */
	public function fields(Model $model, $alias = null, $fields = array(), $quote = true) {
		if (empty($alias)) {
			$alias = $model->alias;
		}
		$fields = parent::fields($model, $alias, $fields, false);
		$count = count($fields);

		if ($count >= 1 && strpos($fields[0], 'COUNT(*)') === false) {
			$result = array();
			for ($i = 0; $i < $count; $i++) {
				$prepend = '';

				if (strpos($fields[$i], 'DISTINCT') !== false && strpos($fields[$i], 'COUNT') === false) {
					$prepend = 'DISTINCT ';
					$fields[$i] = trim(str_replace('DISTINCT', '', $fields[$i]));
				}

				if (!preg_match('/\s+AS\s+/i', $fields[$i])) {
					if (substr($fields[$i], -1) === '*') {
						if (strpos($fields[$i], '.') !== false && $fields[$i] != $alias . '.*') {
							$build = explode('.', $fields[$i]);
							$AssociatedModel = $model->{$build[0]};
						} else {
							$AssociatedModel = $model;
						}

						$_fields = $this->fields($AssociatedModel, $AssociatedModel->alias, array_keys($AssociatedModel->schema()));
						$result = array_merge($result, $_fields);
						continue;
					}

					if (strpos($fields[$i], '.') === false) {
						$this->_fieldMappings[$alias . '__' . $fields[$i]] = $alias . '.' . $fields[$i];
						$fieldName = $this->name($alias . '.' . $fields[$i]);
						$fieldAlias = $this->name($alias . '__' . $fields[$i]);
					} else {
						$build = explode('.', $fields[$i]);
						$build[0] = trim($build[0], '[]');
						$build[1] = trim($build[1], '[]');
						$name = $build[0] . '.' . $build[1];
						$alias = $build[0] . '__' . $build[1];

						$this->_fieldMappings[$alias] = $name;
						$fieldName = $this->name($name);
						$fieldAlias = $this->name($alias);
					}
					if ($model->getColumnType($fields[$i]) === 'datetime') {
						$fieldName = "CONVERT(VARCHAR(20), {$fieldName}, 20)";
					}
					$fields[$i] = "{$fieldName} AS {$fieldAlias}";
				}
				$result[] = $prepend . $fields[$i];
			}
			return $result;
		}
		return $fields;
	}

/**
 * Generates and executes an SQL INSERT statement for given model, fields, and values.
 * Removes Identity (primary key) column from update data before returning to parent, if
 * value is empty.
 *
 * @param Model $model
 * @param array $fields
 * @param array $values
 * @return array
 */
	public function create(Model $model, $fields = null, $values = null) {
		if (!empty($values)) {
			$fields = array_combine($fields, $values);
		}
		$primaryKey = $this->_getPrimaryKey($model);

		if (array_key_exists($primaryKey, $fields)) {
			if (empty($fields[$primaryKey])) {
				unset($fields[$primaryKey]);
			}
		}
		$result = parent::create($model, array_keys($fields), array_values($fields));
		return $result;
	}

/**
 * Generates and executes an SQL UPDATE statement for given model, fields, and values.
 * Removes Identity (primary key) column from update data before returning to parent.
 *
 * @param Model $model
 * @param array $fields
 * @param array $values
 * @param mixed $conditions
 * @return array
 */
	public function update(Model $model, $fields = array(), $values = null, $conditions = null) {
		if (!empty($values)) {
			$fields = array_combine($fields, $values);
		}
		if (isset($fields[$model->primaryKey])) {
			unset($fields[$model->primaryKey]);
		}
		if (empty($fields)) {
			return true;
		}
		return parent::update($model, array_keys($fields), array_values($fields), $conditions);
	}

/**
 * Converts database-layer column types to basic types
 *
 * @param mixed $real Either the string value of the fields type.
 *    or the Result object from Sqlserver::describe()
 * @return string Abstract column type (i.e. "string")
 */
	public function column($real) {
		$limit = null;
		$col = $real;
		if (is_object($real) && isset($real->Field)) {
			$limit = $real->Length;
			$col = strtolower($real->Type);
		}
		$col = str_replace(')', '', $col);
		if (strpos($col, '(') !== false) {
			list($col, $limit) = explode('(', $col);
		}

		if (in_array($col, array('date', 'timestamp'))) {
			return $col;
		}
		if (strpos($col, 'number') !== false) {
			return 'integer';
		}
		if (strpos($col, 'integer') !== false) {
			return 'integer';
		}
		if (strpos($col, 'char') !== false) {
			return 'string';
		}
		if (strpos($col, 'text') !== false) {
			return 'text';
		}
		if (strpos($col, 'blob') !== false) {
			return 'binary';
		}
		if (in_array($col, array('float', 'double', 'decimal'))) {
			return 'float';
		}
		if ($col == 'boolean') {
			return $col;
		}
		return 'text';
	}

/**
 * Handle SQLServer specific length properties.
 * SQLServer handles text types as nvarchar/varchar with a length of -1.
 *
 * @param mixed $length Either the length as a string, or a Column descriptor object.
 * @return mixed null|integer with length of column.
 */
	public function length($length) {
		if (is_object($length) && isset($length->Length)) {
			$type = strtolower($length->Type);
			if ($length->Size > 0) {
				return $length->Precision . ',' . $length->Size;
			}
			if (strpos($type, 'number') !== false) {
				if (!empty($length->Precision))
					return $length->Precision;
				else
					return '11';
			}
			if (strpos($type, 'date') !== false) {
				return null;
			}
			return $length->Length;
		}
		return parent::length($length);
	}

/**
 * Builds a map of the columns contained in a result
 *
 * @param PDOStatement $results
 * @return void
 */
	public function resultSet($results) {
		$this->results = $results;
		$this->map = array();
		$numFields = $results->columnCount();
		$index = 0;
		$j = 0;

		//PDO::getColumnMeta is experimental and does not work with oci,
		//	so try to figure it out based on the querystring
		$querystring = $results->queryString;
		if (strpos($querystring, 'cake_paging') !== false) {
			$querystring = preg_replace('/^SELECT \* FROM \((.*)\) cake_paging.+$/s', '$1', $querystring);
			$querystring = trim($querystring);
		}

		if (stripos($querystring, 'SELECT') === 0) {
			$last = strripos($querystring, 'FROM');
			if ($last !== false) {
				$selectpart = substr($querystring, 7, $last - 8);
				$selects = String::tokenize($selectpart, ',', '(', ')');
			}
		} elseif (strpos($querystring, 'PRAGMA table_info') === 0) {
			$selects = array('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk');
		} elseif (strpos($querystring, 'PRAGMA index_list') === 0) {
			$selects = array('seq', 'name', 'unique');
		} elseif (strpos($querystring, 'PRAGMA index_info') === 0) {
			$selects = array('seqno', 'cid', 'name');
		}
		while ($j < $numFields) {
			if (!isset($selects[$j])) {
				$j++;
				continue;
			}
			if (preg_match('/\bAS\s+(.*)/i', $selects[$j], $matches)) {
				$columnName = trim($matches[1], '"');
			} else {
				$columnName = trim(str_replace('"', '', $selects[$j]));
			}

			if (strpos($selects[$j], 'DISTINCT') === 0) {
				$columnName = str_ireplace('DISTINCT', '', $columnName);
			}

			$metaType = false;
			try {
				$metaData = (array)$results->getColumnMeta($j);
				if (!empty($metaData['oci:decl_type'])) {
					$metaType = trim($metaData['oci:decl_type']);
				}
			} catch (Exception $e) {
			}

			if (strpos($columnName, '__')) {
				$parts = explode('__', $columnName);
				$this->map[$index++] = array(trim($parts[0]), trim($parts[1]), $metaType);
			} else {
				$this->map[$index++] = array(0, $columnName, $metaType);
			}
			$j++;
		}
	}

/**
 * Returns the ID generated from the previous INSERT operation.
 *
 * @param mixed $source
 * @return mixed
 */
	public function lastInsertId($source = null) {
		$sequence = $source . '_seq';
		$sql = "SELECT $sequence.currval FROM dual";
		$result = $this->_execute($sql);

		if (!$result) {
			$result->closeCursor();
			return false;
		}

		while ($row = $result->fetch(PDO::FETCH_NUM)) {
			return $row[0];
		}
		return false;
	}

/**
 * Builds final SQL statement
 *
 * @param string $type Query type
 * @param array $data Query data
 * @return string
 */
	public function renderStatement($type, $data) {
		switch (strtolower($type)) {
			case 'select':
				extract($data);
				$fields = trim($fields);
				
				if ($limit) {
					if (!$order) {
						$order = 'ORDER BY NULL';
					}
					preg_match('/\s*LIMIT\s+(\d+)(,\s*(\d+)){0,1}$/', $limit, $limitOffset);
					
					if (isset($limitOffset[3])) {
						$limit = intval($limitOffset[3]);
						$offset = intval($limitOffset[1]);
					} else {
						$limit = intval($limitOffset[1]);
						$offset = 0;
					}
					$start = $offset + 1;
					$end = $offset + $limit;
					$rowCounter = self::ROW_COUNTER;
					$sql = "SELECT * FROM (
							SELECT {$fields}, ROW_NUMBER() OVER ({$order}) AS {$rowCounter}
							FROM {$table} {$alias} {$joins} {$conditions} {$group}
						) cake_paging
						WHERE cake_paging.{$rowCounter} BETWEEN {$start} AND {$end}";
					return trim($sql);
				}
				return trim("SELECT {$fields} FROM {$table} {$alias} {$joins} {$conditions} {$group} {$order}");
			case "schema":
				extract($data);

				foreach ($indexes as $i => $index) {
					if (preg_match('/PRIMARY KEY/', $index)) {
						unset($indexes[$i]);
						break;
					}
				}

				foreach (array('columns', 'indexes') as $var) {
					if (is_array(${$var})) {
						${$var} = "\t" . implode(",\n\t", array_filter(${$var}));
					}
				}
				return trim("CREATE TABLE {$table} (\n{$columns});\n{$indexes}");
			default:
				return parent::renderStatement($type, $data);
		}
	}

/**
 * Returns a quoted and escaped string of $data for use in an SQL statement.
 *
 * @param string $data String to be prepared for use in an SQL statement
 * @param string $column The column into which this data will be inserted
 * @return string Quoted and escaped data
 */
	public function value($data, $column = null) {
		if ($data === null || is_array($data) || is_object($data)) {
			return parent::value($data, $column);
		}
		if (in_array($data, array('{$__cakeID__$}', '{$__cakeForeignKey__$}'), true)) {
			return $data;
		}

		if (empty($column)) {
			$column = $this->introspectType($data);
		}

		switch ($column) {
			case 'string':
			case 'text':
				return 'N' . $this->_connection->quote($data, PDO::PARAM_STR);
			default:
				return parent::value($data, $column);
		}
	}

/**
 * Returns an array of all result rows for a given SQL query.
 * Returns false if no rows matched.
 *
 * @param Model $model
 * @param array $queryData
 * @param integer $recursive
 * @return array|false Array of resultset rows, or false if no rows matched
 */
	public function read(Model $model, $queryData = array(), $recursive = null) {
		$results = parent::read($model, $queryData, $recursive);
		$this->_fieldMappings = array();
		return $results;
	}

/**
 * Fetches the next row from the current result set.
 * Eats the magic ROW_COUNTER variable.
 *
 * @return mixed
 */
	public function fetchResult() {
		if ($row = $this->_result->fetch(PDO::FETCH_NUM)) {
			$resultRow = array();
			foreach ($this->map as $col => $meta) {
				list($table, $column, $type) = $meta;
				if ($table === 0 && $column === self::ROW_COUNTER) {
					continue;
				}
				$resultRow[$table][$column] = $row[$col];
				if ($type === 'boolean' && $row[$col] !== null) {
					$resultRow[$table][$column] = $this->boolean($resultRow[$table][$column]);
				}
			}
			return $resultRow;
		}
		$this->_result->closeCursor();
		return false;
	}

/**
 * Makes sure it will return the primary key
 *
 * @param Model|string $model Model instance of table name
 * @return string
 */
	protected function _getPrimaryKey($model) {
		$schema = $this->describe($model);
		foreach ($schema as $field => $props) {
			if (isset($props['key']) && $props['key'] === 'primary') {
				return $field;
			}
		}
		return null;
	}

/**
 * Returns number of affected rows in previous database operation. If no previous operation exists,
 * this returns false.
 *
 * @param mixed $source
 * @return integer Number of affected rows
 */
	public function lastAffected($source = null) {
		$affected = parent::lastAffected();
		if ($affected === null && $this->_lastAffected !== false) {
			return $this->_lastAffected;
		}
		return $affected;
	}

/**
 * Executes given SQL statement.
 *
 * @param string $sql SQL statement
 * @param array $params list of params to be bound to query (supported only in select)
 * @param array $prepareOptions Options to be used in the prepare statement
 * @return mixed PDOStatement if query executes with no problem, true as the result of a successful, false on error
 * query returning no rows, such as a CREATE statement, false otherwise
 * @throws PDOException
 */
	protected function _execute($sql, $params = array(), $prepareOptions = array()) {
		$this->_lastAffected = false;
		if (strncasecmp($sql, 'SELECT', 6) === 0 || preg_match('/^EXEC(?:UTE)?\s/mi', $sql) > 0) {
			return parent::_execute($sql, $params);
		}
		try {
			$this->_lastAffected = $this->_connection->exec($sql);
			if ($this->_lastAffected === false) {
				$this->_results = null;
				$error = $this->_connection->errorInfo();
				$this->error = $error[2];
				return false;
			}
			return true;
		} catch (PDOException $e) {
			if (isset($query->queryString)) {
				$e->queryString = $query->queryString;
			} else {
				$e->queryString = $sql;
			}
			throw $e;
		}
	}
}
