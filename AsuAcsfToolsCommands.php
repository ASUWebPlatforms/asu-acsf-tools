<?php

/**
 * @file
 */

namespace Drush\Commands\acsf_tools;

use Consolidation\AnnotatedCommand\CommandData;
use Consolidation\OutputFormatters\StructuredData\RowsOfFields;

/**
 * A Drush commandfile.
 */
class AsuAcsfToolsCommands extends AcsfToolsCommands {

  /**
   * Runs the passed drush command against all the sites of the factory (mlc stands for ml + concurrent).
   *
   * @command asu-acsf-tools:mlc
   *
   * @aliases asusfmlc,asu-acsf-tools-mlc
   *
   * @asu-acsf-tools-alias
   *
   * @bootstrap site
   *
   * @params $cmd
   *   The drush command you want to run against all sites in your factory.
   * @params $command_args Optional.
   *   A quoted, space delimited set of arguments to pass to your drush command.
   * @params $command_options Optional.
   *   A quoted space delimited set of options to pass to your drush command.
   *
   * @option domain-pattern
   *   Pattern / keyword to check for choosing the domain for uri parameter.
   * @option use-https
   *   Use secure urls for drush commands.
   * @option concurrency-limit
   *   The maximum number of commands to run in parallel. 0 for no limit.
   * @option sites-filter
   *   Filter the sites which the command will be executed on. It uses the same format as the --filter option. Possible fields to filter on:
   *   name, site_id, db_name, domain [default: name]
   * @option alias
   *   The drush alias to execute the given command on. It will download a local copy of the remote sites.json file and use it to generate
   *   the drush commands to be executed using the given alias. Useful when acsf-tools is not installed on the remote factory.
   * @option alias-refresh
   *   Force the refresh of the local of the sites.json for the given alias.
   *
   * @table-style default
   *
   * @field-labels
   *   status: Command status
   *   result: Command result
   *   domain: Domain
   *   db_name: DB name
   *   name: Site name
   *   site_id: Site ID
   * @default-fields name,result
   *
   * @filter-default-field result
   *
   * @return RowsOfFields
   */
  public function mlc($cmd, $command_args = '', $command_options = '', $options = [
    'domain-pattern' => '',
    'use-https' => 0,
    'concurrency-limit' => 0,
    'format' => self::FORMAT_PROGRESS,
    'sites-filter' => self::REQ,
    'alias' => self::REQ,
    'alias-refresh' => false,
  ]) {
    // Exit early if there is no sites.
    $sites = $this->getSites();
    if (!$sites || empty($sites)) {
      if ($options['format'] === self::FORMAT_PROGRESS) {
        $this->logger()->error('Impossible to fetch the list of sites. If you are not on an ACSF instance, use the --alias option.');
      }

      return;
    }

    // Avoid warning due to inconsistent parameters.
    if ($options['format'] === self::FORMAT_PROGRESS) {
      $options['filter'] = NULL;
      $options['field'] = NULL;
      $options['fields'] = NULL;
    }

    // Filter the sites which the command will be executed on.
    $sites = $this->filterSites($sites, $options['sites-filter']);

    // Prepare the arguments and options of the drush command that will get
    // executed on the sites.
    $drush_command_args = $this->getCommandArgs($command_args);
    $drush_command_options = $this->getCommandOptions($command_options);

    // Command always passes the default option as `yes` irrespective if `--no`
    // option used. Pass confirmation as `no` if use that.
    if ($options['no']) {
      $drush_command_options['no'] = TRUE;
    }

    $processes = [];
    $rows = [];

    // Prepare the commands to execute on the sites.
    foreach ($sites as $key => $details) {
      // Determine the domain to use for the --uri option.
      $sites[$key]['domain'] = $this->getDomain($details, $options);

      $rows[$key] = [
        'status' => NULL,
        'result' => NULL,
        'domain' => $sites[$key]['domain'],
        'db_name' => $key,
        'name' => $details['machine_name'],
        'site_id' => $details['conf']['gardens_site_id'],
      ];

      $process = $this->prepareCommand($sites[$key]['domain'], $details, $cmd, $drush_command_args, $drush_command_options);
      if ($process === self::SITE_NOT_READY) {
        $rows[$key]['status'] = self::STATUS_SKIP;
        continue;
      }

      $processes[$key] = $process;
    }

    while (!empty($processes)) {
      $running = 0;

      foreach ($processes as $key => $process) {
        // If the process is still running, we simply count it and move to the
        // next one.
        if ($process->isRunning()) {
          $running++;
          continue;
        }

        // If the process is completed, we fetch the result and remove the
        // process from the queue.
        if ($process->isTerminated()) {
          unset($processes[$key]);

          $rows[$key]['status'] = !$process->isSuccessful() ? self::STATUS_ERROR : self::STATUS_SUCCESS;
          $rows[$key]['result'] = trim(rtrim($process->getOutput())) . trim(rtrim($process->getErrorOutput()));

          if ($options['format'] === self::FORMAT_PROGRESS) {
            if ($process->isSuccessful()) {
              $this->output()->writeln("\n=> The command executed successfully for the site " . $sites[$key]['machine_name'] . '.');
              $this->output()->writeln($process->getOutput());
              $this->output()->writeln($process->getErrorOutput());
            }
            else {
              $this->output()->writeln("\n=> The command failed to execute for the site " . $sites[$key]['machine_name'] . '.');
              $this->output()->writeln($process->getErrorOutput());
            }
          }

          continue;
        }

        // If we have not reached the concurrency limit yet and the process has
        // not started yet, start it.
        if (!$process->isStarted() && (!intval($options['concurrency-limit']) || $options['concurrency-limit'] > $running)) {
          if ($options['format'] === self::FORMAT_PROGRESS) {
            $this->output()->writeln("\n=> Executing command on " . $sites[$key]['machine_name']);
          }
          $process->start();

          $running++;
        }
      }
    }

    return $options['format'] === self::FORMAT_PROGRESS ? new RowsOfFields([]) : new RowsOfFields(array_values($rows));
  }

  /**
   * Validate the options related to alias management.
   *
   * @hook validate @asu-acsf-tools-alias
   *
   * @throws \Exception
   */
  public function asuAliasValidate(CommandData $commandData) {
    parent::AliasValidate($commandData);
  }

  /**
   * Prepare the alias if given to the command.
   *
   * @hook pre-command @asu-acsf-tools-alias
   */
  public function asuAliasPrepare(CommandData $commandData) {
    parent::AliasPrepare($commandData);
  }

}
