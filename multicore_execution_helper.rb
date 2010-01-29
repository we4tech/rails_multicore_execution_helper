module MulticoreExecutionHelper

  def execute_in_multicores(
      p_cores, p_total_rows, p_model, p_conditions = {}, &block)

    p_cores == 2 if p_cores.to_i == 0    
    total_items_per_core = p_total_rows / p_cores
    logger.info "[BATCH-PROCESS-LOG] Total processes - #{p_cores}, " +
                "total rows - #{p_total_rows} [#{total_items_per_core} / 1 core]"

    # Create job id for each process
    job_ids = p_cores.times.collect{|i| rand.to_s }

    # Fork process for each core and execute the block
    p_cores.times do |offset|
      Process.fork do
        logger.info "[BATCH-PROCESS-LOG] Starting thread - #{offset} " +
                    "assigned # #{job_ids[offset]}"

        # Keep job track through the created process pid file.
        pid_file = File.join(RAILS_ROOT, 'tmp/pids/', "#{job_ids[offset]}.pid")
        File.open(pid_file, 'w') {|f| f.puts Process.pid.to_s}

        # Since fork process is created from the sample of the parent
        # process's memory so we need to reconnect all live connections.
        begin
          ActiveRecord::Base.connection.reconnect!

          # Retrieve data from the specific row through the defined
          # offset and limit
          teams = p_model.find(
              :all, {
                  :offset => (offset * total_items_per_core),
                  :limit => total_items_per_core}.merge(p_conditions))

          block.call(teams)
        rescue => $e
          logger.error "[BATCH-PROCESS-LOG] Exception raised during " +
                       "execution - #{$e.inspect}"
        end

        # Remove pid since we are done here!
        FileUtils.rm(pid_file)
      end
    end

    # monitor whether the process is completed or still in progress
    # don't return this method unless all the forked processes have
    # completed their job
    sleep(2)

    while 1 do
      fully_completed = true
      for job_id in job_ids
        pid_file = File.join(RAILS_ROOT, 'tmp/pids/', "#{job_id}.pid")
        if fully_completed && File.exists?(pid_file)
          fully_completed = false
          break
        end
      end

      break if fully_completed
      sleep(2)
      logger.debug '[BATCH-PROCESS-LOG] again...'
    end
  end

end
