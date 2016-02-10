cakephp-2.x_oracle-driver
=========================
This is the CakePHP-2.x Oracle database DBO driver. It was tested with CakePHP version 2.8.0 and it works fine.

## Requirements

The master branch has the following requirements:

* CakePHP 2.x or greater.
* PHP 5.3.0 or greater.

## Installation

* Clone/Copy the files in this directory into `app\Model\Datasource\Database\`
* Set database config file `app\Config\database.php` as :
```
<?php
class DATABASE_CONFIG {
	public $default = array(
		'datasource' => 'Database/Oracle',
		'persistent' => false,
		'login' => 'username',
		'password' => 'pass',
		'database' => 'hostname/databasename',
		'prefix' => '',
		'encoding' => 'utf8',
	);
}
```

## Known Issues


* With utf8 charset, when a varchar2 field  if full and contains localized chars this warning is raised:

  Warning (2): PDOStatement::fetch(): column 5 data was too large for buffer and was truncated to fit it
  [APP\Model\Datasource\Database\Oracle.php, line 662]

  Thats is not a bug from this driver, is a PDO_OCI error:
  https://bugs.php.net/bug.php?id=54379&edit=1
