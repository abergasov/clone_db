#!/usr/bin/env php
<?php

require __DIR__ . '/vendor/autoload.php';

use Console\Command\CloneMySql;
use Phinx\Console\Command;
use Symfony\Component\Console\Application;

use Symfony\Component\Yaml\Yaml;


$value = Yaml::parse(file_get_contents(__DIR__ . '/.env.yml'));

$clon = new CloneMySql('clone_mysql');
$clon->getConfig($value);

$app = new Application();
$app->addCommands([
    new Command\Init('init'),
    new Command\Create('create'),
    new Command\Migrate('migrate'),
    new Command\Rollback('rollback'),
    new Command\Status('status'),
    new Command\Breakpoint('breakpoint'),
    new Command\Test('test'),
    new Command\SeedCreate('seedcreate'),
    new Command\SeedRun('seedrun'),

    $clon,
]);
$app->run();