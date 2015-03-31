cakephp-2.x_oracle-driver
=========================
This is the CakePHP-2.x Oracle database DBO driver. It was tested with CakePHP vertion 2.4.6 and it works fine.

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
