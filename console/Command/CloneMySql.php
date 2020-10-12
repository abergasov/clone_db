<?php

namespace Console\Command;

use ParagonIE\EasyDB\EasyDB;
use ParagonIE\EasyDB\Factory;
use PDO;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\OutputInterface;

class CloneMySql extends Command {

    const PROGRESS_FILE = 'pgr.txt';
    const ROWS_LIMIT = 5000;
    const INSERT_LIMIT = 20;

    private $conf;
    /**
     * @var EasyDB
     */
    private $localConnect;
    private $progressExec = [];
    /**
     * @var ConsoleLogger
     */
    private $logger;

    public function getConfig(array $conf) {
        $this->conf = $conf;
    }

    protected function configure() {
        $this->setName('clone_mysql')->setDescription('Clone from test 2 local');
    }

    protected function execute(InputInterface $input, OutputInterface $output) {
        $this->logger = new ConsoleLogger($output);
        if (!isset($this->conf['databases']['mysql'])) {
            $this->logger->critical('cant find config for remote mysql');
            return 1;
        }
        $this->validateLocalConnection();
        $this->loadProgress();
        foreach ($this->conf['databases']['mysql'] as $name => &$rmtDb) {
            $rmtDb['connect'] = $this->validateRemoteConnection($rmtDb, $name);
            $this->scanBase($rmtDb['connect'], $rmtDb, $name);
        }
        return 1;
    }

    private function scanBase(EasyDB $connect, array $dbConf, string $name) {
        $sql = "SELECT table_name, table_rows, DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?";
        $result = $connect->run($sql, $name);
        if (!is_array($result)) {
            $this->logger->warning("expected array for $name. scan base will skip");
            return;
        }
        foreach ($result as $table) {
            $name = $table['table_name'];
            // проверяем, что б таблица не была в списке неотслеживаемых
            if (in_array($name, $dbConf['ignore_tables'])) {
                $this->logger->notice("table {$name} is in ignore list");
                continue;
            }
            $tblOk = isset($this->progressExec[$dbConf['name']]) && isset($this->progressExec[$dbConf['name']][$name]);
            if (!$tblOk) {
                $this->logger->info("{$name} - start create table");
                $this->processTable($name, $dbConf, $connect);
                $this->logger->info("{$name} - create table ok");
            }
            // проверяем, что не надо из таблицы тянуть данные
            if (in_array($name, $dbConf['ignore_data_from_tables'])) {
                $this->logger->info("{$name} - skip data for table");
                continue;
            }
            $tblOk = isset($this->progressExec[$dbConf['name']]) && isset($this->progressExec[$dbConf['name']][$name]);
            if ($tblOk) {
                $this->logger->info("{$name} - start copy rows");
                $this->processTableRows($name, $dbConf, $connect);
                $rows = $this->progressExec[$dbConf['name']][$name]['rows'];
                $this->logger->info("{$name} - copy rows is done (~${rows})");
            }
        }
    }

    private function processTable(string $tableName, array $params, EasyDB $connect) {
        $generateSql = $connect->run("SHOW CREATE TABLE `${tableName}`");
        if (!is_array($generateSql)) {
            return;
        }
        if (count($generateSql) === 0) {
            return;
        }

        if (!isset($generateSql[0]['Create Table'])) {
            return;
        }
        $sql = $generateSql[0]['Create Table'];
        if (substr($sql, 0, 12) !== "CREATE TABLE") {
            return;
        }

        // удаляем в локальной бд таблицу
        $this->localConnect->run('SET FOREIGN_KEY_CHECKS=0');
        $this->localConnect->run('DROP TABLE IF EXISTS ' . $tableName);
        $this->localConnect->run('SET FOREIGN_KEY_CHECKS=1');
        $this->localConnect->run($sql);
        $this->logProgressTableCreation($params['name'], $tableName);
    }

    private function processTableRows(string $tableName, array $params, EasyDB $connect) {
        $rows = $this->progressExec[$params['name']][$tableName]['rows']; // сколько строк уже обработано
        if (in_array($tableName, $params['get_last_rows'])) {
            if ($rows > self::ROWS_LIMIT) {
                return;
            }
        }
        $res = $connect->run("SHOW KEYS FROM `{$tableName}` WHERE Key_name = 'PRIMARY'");
        if (!is_array($res)) {
            $this->logger->error("{$tableName} - can't get primary key 4 table");
            return;
        }
        $key = null;
        if (count($res) === 0) {
            $ds = $connect->run("SHOW COLUMNS FROM `{$tableName}` LIKE 'id'");
            if (count($ds) > 0) {
                $key = 'id';
            } else {
                return;
            }
        } else {
            $key = $res[0]['Column_name'];
        }
        if (is_null($key)) {
            return;
        }
        $offset = $this->progressExec[$params['name']][$tableName]['rows'];
        $sql = "SELECT * FROM `{$tableName}` ORDER BY `${key}` DESC LIMIT ?, ?";
        $result = $connect->run($sql, $offset, self::ROWS_LIMIT);
        if (!is_array($result)) {
            $this->logger->error("{$tableName} - expect array");
            return;
        }
        if (count($result) === 0) {
            $this->logProgressTableInsert($params['name'], $tableName, 0, true);
            return;
        }
        $this->insertRows($result, $tableName);
        $this->logProgressTableInsert($params['name'], $tableName, self::ROWS_LIMIT, count($result) < self::ROWS_LIMIT);
        if (in_array($tableName, $params['get_last_rows'])) {
            // если интересовало только последние строки выгрузить
            return;
        }
        if (count($result) < self::ROWS_LIMIT) {
            // выгрузили все, что было
            return;
        }
        unset($result);
        return $this->processTableRows($tableName, $params, $connect);
    }

    private function insertRows(array $result, string $tableName) {
        $columns = array_map(function ($e) {
            return '`' . $e . '`';
        }, array_keys($result[0]));

        $sql = "INSERT IGNORE INTO {$tableName} (" . implode(',', $columns) . ") VALUES ";
        $sqlAppend = [];
        $values = [];
        foreach ($result as $row) {
            array_push($values, ...array_values($row));
            $sqlAppend[] = '(' . implode(',', array_fill(0, count($row), '?')) . ')';
            if (count($sqlAppend) >= self::INSERT_LIMIT) {
                $this->localConnect->run($sql . implode(',', $sqlAppend), ...$values);
                $sqlAppend = [];
                $values = [];
            }
        }
        if (count($sqlAppend) > 0) {
            $this->localConnect->run($sql . implode(',', $sqlAppend), ...$values);
        }
    }

    private function logProgressTableInsert(string $connectName, string $table, int $rows, bool $done = false) {
        $this->progressExec[$connectName][$table]['done'] = $done;
        $this->progressExec[$connectName][$table]['rows'] += $rows;
        $this->saveProgress();
    }

    private function logProgressTableCreation(string $connectName, string $table) {
        if (!isset($this->progressExec[$connectName])) {
            $this->progressExec[$connectName] = [];
        }
        $this->progressExec[$connectName][$table] = [
            'created' => true,
            'rows' => 0,
            'done' => false,
        ];
        $this->saveProgress();
    }

    private function saveProgress() {
        file_put_contents(__DIR__ . '/' . self::PROGRESS_FILE, json_encode($this->progressExec));
    }

    private function loadProgress() {
        $prg = [];
        if (file_exists(__DIR__ . '/' . self::PROGRESS_FILE)) {
            $dt = file_get_contents(__DIR__ . '/' . self::PROGRESS_FILE);
            $prg = json_decode($dt, true);
        }
        $this->progressExec = $prg;
    }

    private function validateRemoteConnection(array $conf, string $name): ?EasyDB {
        $connect = $this->getConnection($conf['host'], $conf['port'], $conf['name'], $conf['user'], $conf['pass']);
        $this->logger->info("connected to {$name}");
        return $connect;
    }

    private function validateLocalConnection(): bool {
        if (!isset($this->conf['local_base'])) {
            $this->logger->error('Invalid config localdb');
            return false;
        }

        $valid = $this->conf['local_base']['host'] === 'localhost' || $this->conf['local_base']['host'] === '127.0.0.1';
        if (!$valid) {
            $this->logger->error('local DB only (localhost / 127.0.0.1)');
            return false;
        }

        $this->localConnect = $this->getConnection(
            $this->conf['local_base']['host'],
            $this->conf['local_base']['port'],
            $this->conf['local_base']['name'],
            $this->conf['local_base']['user'],
            $this->conf['local_base']['pass']
        );
        return $this->localConnect !== null;
    }

    /**
     * @param string $host
     * @param string $port
     * @param string $name
     * @param string $user
     * @param string $pass
     * @return EasyDB|null
     */
    private function getConnection(string $host, string $port, string $name, string $user, string $pass): ?EasyDB {
        return Factory::create("mysql:host={$host};port={$port};dbname={$name}", $user, $pass, [
            PDO::ATTR_PERSISTENT => true
        ]);
    }
}