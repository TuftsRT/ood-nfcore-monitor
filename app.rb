# frozen_string_literal: true

require "json"
require "yaml"
require "time"
require "open3"
require "sinatra/base"

# Nextflow Pipeline Monitor
#
# A `role: web` (Passenger) dashboard that gives a Nextflow-Tower-like view of
# the current user's nf-core pipeline runs launched through Open OnDemand.
#
# It never talks to a database or another user's files. Everything is derived
# from artifacts that already exist under the user's own account:
#
#   1. Discover runs from OnDemand's own per-user session records:
#        ~/ondemand/data/sys/dashboard/batch_connect/db/<id>   -> job_id, token, times
#        .../batch_connect/<token>/output/<id>/connection.yml  -> working_path (launch dir)
#        .../output/<id>/user_defined_context.json             -> outdir, executor, ...
#      Only sessions whose token begins with "sys/nf-core-" are kept.
#
#   2. Read what Nextflow / nf-core write into the launch + output dirs:
#        <workdir>/.nextflow/history                        -> run name, status, duration
#        <outdir>/pipeline_info/execution_trace_*.txt       -> per-task table (live)
#        <outdir>/pipeline_info/execution_report_*.html     -> nf-core's own reports
#        <session>/output.log                               -> live stdout tail
#
#   3. Live scheduler state from `sacct -X -j <job_id>` (the job id comes from the
#      user's own OnDemand db record).
#
# Security: all reads happen as the PUN user (the user themselves), and every
# path is built from the user's own OnDemand records, never from request input.
# Session ids are validated as UUIDs and job ids as digits; sacct is invoked as
# an argv array wrapped in `timeout`, so nothing from a file can reach a shell.
class NfMonitorApp < Sinatra::Base
  enable :logging
  set :root, File.dirname(__FILE__)
  set :method_override, false
  set :protection, except: :path_traversal

  BC_ROOT = File.join(Dir.home, "ondemand", "data", "sys", "dashboard", "batch_connect")
  DB_ROOT = File.join(BC_ROOT, "db")
  # OnDemand deployment environments to scan (batch_connect/<env>/<app>/output/<id>).
  ENVS = %w[sys dev usr].freeze
  APP_GLOB = "nf-core-*"

  # nf-core HTML reports under <outdir>/pipeline_info, by report key.
  REPORT_GLOBS = {
    "report"   => "execution_report_*.html",
    "timeline" => "execution_timeline_*.html",
    "dag"      => "pipeline_dag_*.html"
  }.freeze
  # Report keys the app will serve inline (see serve_report). "multiqc" lives
  # under <outdir>/multiqc rather than pipeline_info, so it's handled separately.
  REPORT_KEYS = (REPORT_GLOBS.keys + %w[multiqc]).freeze

  UUID_PATTERN = /\A[0-9a-fA-F-]{36}\z/.freeze
  JOBID_PATTERN = /\A\d+\z/.freeze
  SACCT_TIMEOUT = 15
  SQUEUE_TIMEOUT = 15
  LOG_TAIL_LINES = 200
  LOG_TAIL_BYTES = 128 * 1024

  # Task statuses reported by Nextflow's trace file, grouped into the buckets we
  # show as progress. Anything not listed is counted as "other".
  # Trace task statuses, grouped into the buckets we report per step and overall.
  FAIL_STATES = %w[FAILED ABORTED].freeze
  PENDING_STATES = %w[SUBMITTED NEW PENDING].freeze

  # --- Discovery -------------------------------------------------------------
  class << self
    # All of the user's nf-core runs, newest first, with a light-weight summary
    # (enough for the index list). Detailed task parsing is deferred to #run_detail.
    #
    # Live scheduler state uses a single `squeue` call for the whole list rather
    # than one `sacct` per run, so the index stays fast with many runs.
    def list_runs
      running = running_jobs
      db = db_index
      session_dirs.map { |sd| run_summary(sd, db, running) }.compact
                  .sort_by { |r| -r[:created_at].to_i }
    end

    # Full detail for one run: identity, live status, task breakdown, per-process
    # progress, resource rollup, report links and a log tail.
    def run_detail(session_id)
      sd = session_dirs.find { |s| s[:session_id] == session_id }
      return nil unless sd

      run = run_context(sd, db_index, running_jobs, deep: true)
      return nil unless run

      trace = parse_trace(run[:trace_file])
      running = %w[running pending].include?(run[:state_class])
      prog = parse_progress(run[:output_log])
      progress = progress_stats(trace[:counts], prog, running)
      {
        id: run[:id],
        pipeline: run[:pipeline],
        run_name: run[:run_name],
        created_at: run[:created_at],
        job_id: run[:job_id],
        job_state: run[:job_state],
        state_class: run[:state_class],
        pipeline_status: run[:pipeline_status],
        elapsed: run[:elapsed],
        executor: run[:executor],
        workdir: run[:workdir],
        outdir: run[:outdir],
        revision: run[:revision],
        session: run[:session],
        command: run[:command],
        nextflow_version: run[:nextflow_version],
        # Bound the payload on big pipelines: recent tasks for the live feed,
        # plus all failed tasks (capped) so the failure panel is never truncated.
        recent_tasks: trace[:tasks].last(300).reverse,
        failed_tasks: trace[:tasks].select { |t| FAIL_STATES.include?(t[:status]) }.last(200),
        counts: trace[:counts],
        progress: progress,
        completion: prog[:finished] ? {
          outcome: prog[:outcome], succeeded: prog[:succeeded], failed: prog[:failed],
          ignored: prog[:ignored], cpu_hours: prog[:cpu_hours], nf_duration: prog[:nf_duration]
        } : nil,
        processes: trace[:processes],
        timeline: trace[:timeline],
        resources: trace[:resources],
        params: parse_params(run[:pipeline_info]),
        versions: parse_versions(run[:pipeline_info]),
        reports: report_links(run[:pipeline_info], run[:outdir], run[:created_at]),
        log_tail: tail_file(run[:output_log])
      }
    end

    private

    # Durable list of the user's nf-core sessions, discovered from the on-disk
    # session directories (which survive far longer than the rotating db/ index):
    #   batch_connect/<env>/<app>/output/<session_id>/
    def session_dirs
      ENVS.flat_map do |env|
        Dir.glob(File.join(BC_ROOT, env, APP_GLOB, "output", "*")).filter_map do |dir|
          session_id = File.basename(dir)
          next unless File.directory?(dir)
          next unless session_id.match?(UUID_PATTERN)

          { dir: dir, session_id: session_id, app: File.basename(File.dirname(File.dirname(dir))), env: env }
        end
      end
    end

    # session_id => db record (job_id, created_at, completed_at) for the recent
    # runs OnDemand still keeps in db/. Absent for older runs; that's fine.
    def db_index
      Dir.glob(File.join(DB_ROOT, "*")).each_with_object({}) do |path, acc|
        next unless File.file?(path)

        data = JSON.parse(File.read(path)) rescue nil
        next unless data.is_a?(Hash) && data["id"]

        acc[data["id"]] = data
      end
    end

    # job_id => scheduler state, from a single squeue call over the user's queue.
    def running_jobs
      user = ENV["USER"].to_s
      return {} if user.empty?

      out, _err, ok = run_capture(
        ["squeue", "-h", "-u", user, "-o", "%i %T"], SQUEUE_TIMEOUT
      )
      return {} unless ok

      out.each_line.each_with_object({}) do |line, acc|
        id, state = line.split
        acc[id] = state if id && id.match?(JOBID_PATTERN)
      end
    end

    # Resolve a session dir into concrete paths + metadata, or nil if unreadable.
    # `deep: true` (detail view) upgrades an ambiguous state with a precise sacct
    # lookup; the list view relies on the shared squeue map to stay fast.
    def run_context(sd, db, running, deep: false)
      dir = sd[:dir]
      conn = load_yaml(File.join(dir, "connection.yml"))
      ctx  = load_json(File.join(dir, "user_defined_context.json"))
      entry = db[sd[:session_id]] || {}

      workdir = presence(conn["working_path"]) || presence(ctx["workdir"]) || Dir.home
      workdir = File.expand_path(workdir)
      outdir_raw = presence(ctx["outdir"]) || "results"
      outdir = File.expand_path(outdir_raw, workdir)
      pipeline_info = File.join(outdir, "pipeline_info")

      job_id = entry["job_id"].to_s
      created_at = entry["created_at"].to_i
      created_at = dir_created_at(dir) if created_at.zero?
      history = history_for(workdir, created_at)

      state, state_class = resolve_state(history[:status], job_id, entry, running, deep)

      {
        id: sd[:session_id],
        pipeline: pipeline_label(sd[:app]),
        job_id: job_id,
        created_at: created_at,
        completed_at: entry["completed_at"],
        executor: presence(ctx["executor"]) || "-",
        workdir: workdir,
        outdir: outdir,
        pipeline_info: pipeline_info,
        output_log: presence(conn["output_log_path"]) || File.join(dir, "output.log"),
        run_name: history[:run_name],
        pipeline_status: history[:status],
        trace_file: latest_trace(pipeline_info),
        job_state: state,
        state_class: state_class,
        elapsed: history[:duration],
        revision: history[:revision],
        session: history[:session],
        command: history[:command],
        nextflow_version: presence(ctx["nextflow_version"])
      }
    end

    def run_summary(sd, db, running)
      run = run_context(sd, db, running)
      return nil unless run

      counts = quick_counts(run[:trace_file])
      # Parse Nextflow's status board only for active runs (few of them); finished
      # runs don't need it since "seen" tasks already equal the true total.
      active = %w[running pending].include?(run[:state_class])
      prog = active ? parse_progress(run[:output_log]) : { finished: true, waiting: 0 }
      {
        id: run[:id],
        pipeline: run[:pipeline],
        run_name: run[:run_name],
        created_at: run[:created_at],
        created_label: run[:created_at].positive? ? Time.at(run[:created_at]).strftime("%Y-%m-%d %H:%M") : "-",
        job_id: run[:job_id],
        job_state: run[:job_state],
        state_class: run[:state_class],
        pipeline_status: run[:pipeline_status],
        elapsed: run[:elapsed],
        executor: run[:executor],
        outdir: run[:outdir],
        counts: counts,
        progress: progress_stats(counts, prog, active)
      }
    end

    # --- Scheduler state -----------------------------------------------------

    # Fold pipeline result + scheduler queue into one display state and CSS class.
    # Precedence: Nextflow's own OK/ERR verdict wins; otherwise the live queue;
    # otherwise (detail view only) sacct; otherwise a best-effort "ended/unknown".
    def resolve_state(pipeline_status, job_id, entry, running, deep)
      return ["COMPLETED", "completed"] if pipeline_status == "OK"
      return ["FAILED", "failed"] if pipeline_status == "ERR"

      if job_id.match?(JOBID_PATTERN) && running[job_id]
        s = running[job_id]
        return [s, s.downcase]
      end

      if deep && job_id.match?(JOBID_PATTERN)
        s = sacct_state(job_id)
        return [s, s.downcase] if s
      end

      return ["ENDED", "other"] if entry["completed_at"]

      ["UNKNOWN", "unknown"]
    end

    # Precise final state for one job (detail view). Returns nil if aged out.
    def sacct_state(job_id)
      out, _err, ok = run_capture(
        ["sacct", "-X", "-n", "-P", "-j", job_id, "--format=State"], SACCT_TIMEOUT
      )
      return nil unless ok

      state = out.lines.map(&:strip).reject(&:empty?).first
      state && !state.empty? ? state.split(/\s+/).first : nil
    end

    def run_capture(argv, timeout)
      stdout, stderr, status = Open3.capture3("timeout", timeout.to_s, *argv)
      [stdout, stderr, status.success?]
    rescue Errno::ENOENT => e
      ["", "command not found: #{e.message}", false]
    end

    # --- Nextflow history ----------------------------------------------------

    # The .nextflow/history line that best matches this session's start time.
    # A launch dir reused across runs has several lines; pick the one whose start
    # is closest to (and not long after) the OnDemand submission time.
    def history_for(workdir, created_at)
      empty = { run_name: nil, status: nil, duration: nil, revision: nil, session: nil, command: nil }
      path = File.join(workdir, ".nextflow", "history")
      return empty unless File.file?(path)

      # Columns: timestamp, duration, run_name, status, revision_id, session_id, command
      rows = File.readlines(path).filter_map do |line|
        cols = line.chomp.split("\t")
        next if cols.length < 4

        started = Time.parse(cols[0]) rescue nil
        { started: started, duration: cols[1], run_name: cols[2], status: cols[3],
          revision: cols[4], session: cols[5], command: cols[6] }
      end
      return empty if rows.empty?

      chosen =
        if created_at.positive?
          target = Time.at(created_at)
          rows.select { |r| r[:started] }
              .min_by { |r| (r[:started] - target).abs } || rows.last
        else
          rows.last
        end

      { run_name: chosen[:run_name], status: normalize_status(chosen[:status]),
        duration: chosen[:duration], revision: presence(chosen[:revision]),
        session: presence(chosen[:session]), command: presence(chosen[:command]) }
    end

    # Resolved pipeline parameters from nf-core's params_*.json (what actually ran).
    # Returns a sorted [{key, value}] list; nested/array values are compact JSON.
    def parse_params(pipeline_info)
      file = Dir.glob(File.join(pipeline_info, "params_*.json")).max_by { |f| File.mtime(f) }
      return [] unless file

      data = JSON.parse(File.read(file)) rescue nil
      return [] unless data.is_a?(Hash)

      data.sort_by { |k, _| k.to_s }.filter_map do |k, v|
        next if v.nil? || v == ""

        val = v.is_a?(Hash) || v.is_a?(Array) ? JSON.generate(v) : v.to_s
        val = val[0, 300] + "…" if val.length > 300
        { key: k.to_s, value: val }
      end
    end

    # Tool -> version pairs from nf-core's software-versions YAML, de-duplicated.
    def parse_versions(pipeline_info)
      # Filename varies across pipelines/versions: nf_core_<pipe>_software_versions.yml,
      # ..._software_mqc_versions.yml, or collated_versions.yml.
      file = %w[*software*versions*.yml collated_versions.yml *versions*.yml]
             .flat_map { |g| Dir.glob(File.join(pipeline_info, g)) }.first
      return [] unless file && File.file?(file)

      data = YAML.safe_load(File.read(file)) rescue nil
      return [] unless data.is_a?(Hash)

      pairs = {}
      data.each_value do |v|
        if v.is_a?(Hash)
          v.each { |tool, ver| pairs[tool.to_s] ||= ver.to_s }
        end
      end
      # Some files are a flat tool=>version map rather than process=>{tool=>version}.
      data.each { |tool, ver| pairs[tool.to_s] ||= ver.to_s unless ver.is_a?(Hash) }
      pairs.sort.map { |tool, ver| { tool: tool, version: ver } }
    end

    # Path to the MultiQC report if the pipeline produced one.
    # Checks MultiQC's known output locations only (no unbounded tree walk).
    def multiqc_file(outdir)
      patterns = [
        File.join(outdir, "multiqc", "multiqc_report.html"),
        File.join(outdir, "multiqc", "*", "multiqc_report.html"),
        File.join(outdir, "multiqc*", "multiqc_report.html")
      ]
      patterns.flat_map { |p| Dir.glob(p) }.first
    end

    # Resolve the on-disk path of a report for a run, from the user's own OnDemand
    # records only (id is a validated UUID, key is from REPORT_KEYS) — never from a
    # request-supplied path. Returns nil if the run or file doesn't exist.
    def report_file(session_id, key)
      return nil unless session_id.to_s.match?(UUID_PATTERN) && REPORT_KEYS.include?(key)

      sd = session_dirs.find { |s| s[:session_id] == session_id }
      return nil unless sd

      run = run_context(sd, db_index, running_jobs, deep: false)
      return nil unless run

      if key == "multiqc"
        f = multiqc_file(run[:outdir])
        f if f && report_current?(f, run[:created_at])
      else
        Dir.glob(File.join(run[:pipeline_info], REPORT_GLOBS[key]))
           .select { |f| report_current?(f, run[:created_at]) }
           .max_by { |f| File.mtime(f) }
      end
    end

    # Reports live in an output directory that is often reused across runs, so an
    # old MultiQC/timeline/report from a previous run may still be on disk while
    # the current run hasn't produced one yet. A report belongs to the current run
    # only if it was written at/after this run started; otherwise it's a leftover.
    # (When the start time is unknown — e.g. an aged-out db record — we don't
    # filter, since we can't tell stale from current.)
    def report_current?(file, created_at)
      start = created_at.to_i
      return true unless start.positive?

      File.mtime(file).to_i >= start
    rescue StandardError
      false
    end

    def normalize_status(raw)
      case raw.to_s
      when "OK" then "OK"
      when "ERR" then "ERR"
      else raw.to_s == "-" ? nil : raw.to_s
      end
    end

    # --- Trace file ----------------------------------------------------------

    def latest_trace(pipeline_info)
      Dir.glob(File.join(pipeline_info, "execution_trace_*.txt")).max_by { |f| File.mtime(f) }
    end

    # Fast status tally for the index (counts only, no per-task retention).
    def quick_counts(trace_file)
      return empty_counts unless trace_file && File.file?(trace_file)

      counts = empty_counts
      first = true
      File.foreach(trace_file) do |line|
        if first
          first = false
          next # header
        end
        cols = line.split("\t")
        counts[bucket_for(cols[4].to_s)] += 1
        counts[:total] += 1
      end
      counts
    end

    # Full parse: retains per-task rows, per-process rollup, and resource totals.
    def parse_trace(trace_file)
      return { tasks: [], counts: empty_counts, processes: [], resources: empty_resources } unless
        trace_file && File.file?(trace_file)

      header = nil
      tasks = []
      counts = empty_counts
      processes = Hash.new { |h, k| h[k] = new_proc(k) }
      overall = new_proc(nil)
      earliest = nil   # min submit time (epoch) -> pipeline wall-clock start
      latest = nil     # max (submit + duration) -> wall-clock end

      File.foreach(trace_file) do |line|
        cols = line.chomp.split("\t")
        if header.nil?
          header = cols
          next
        end
        row = header.zip(cols).to_h
        status = row["status"].to_s
        bucket = bucket_for(status)
        counts[bucket] += 1
        counts[:total] += 1

        metrics = {
          rt: parse_duration(row["realtime"]),
          cpu: parse_percent(row["%cpu"]),
          rss: parse_size_mb(row["peak_rss"]),
          vmem: parse_size_mb(row["peak_vmem"]),
          rchar: parse_size_mb(row["rchar"]),
          wchar: parse_size_mb(row["wchar"])
        }
        proc_name = base_process_name(row["name"])
        pacc = processes[proc_name]
        tally(pacc, bucket, metrics)
        tally(overall, bucket, metrics)

        if (submit = parse_time(row["submit"]))
          earliest = submit if earliest.nil? || submit < earliest
          finish = submit + parse_duration(row["duration"])
          latest = finish if latest.nil? || finish > latest
          pacc[:t_start] = submit if pacc[:t_start].nil? || submit < pacc[:t_start]
          pacc[:t_end] = finish if pacc[:t_end].nil? || finish > pacc[:t_end]
        end

        tasks << {
          task_id: row["task_id"],
          hash: row["hash"],
          name: row["name"],
          process: proc_name,
          status: status,
          exit: row["exit"],
          duration: row["duration"],
          realtime: row["realtime"],
          cpu: row["%cpu"],
          peak_rss: row["peak_rss"]
        }
      end

      proc_list = processes.values.sort_by { |p| p[:name] }.map { |p| finalize_proc(p) }
      o = finalize_proc(overall)
      wall = (earliest && latest && latest > earliest) ? (latest - earliest).round : nil

      # Per-step execution windows relative to the run start, for the Gantt view.
      base = earliest || 0
      timeline = processes.values.filter_map do |p|
        next unless p[:t_start]

        { name: p[:name], start_s: (p[:t_start] - base).round, end_s: (p[:t_end] - base).round,
          failed: p[:failed], total: p[:total] }
      end.sort_by { |x| [x[:start_s], x[:end_s]] }

      {
        tasks: tasks,
        counts: counts,
        processes: proc_list,
        timeline: timeline,
        resources: {
          realtime_sum_s: o[:time_total_s],
          cpu_avg: o[:cpu_avg],
          peak_rss_mb: o[:mem_max_mb],
          peak_vmem_mb: o[:vmem_max_mb],
          io_read_mb: o[:io_read_mb],
          io_write_mb: o[:io_write_mb],
          wall_s: wall,
          # How much the run parallelised: total compute time / wall-clock span.
          parallelism: (wall && wall.positive?) ? (o[:time_total_s].to_f / wall).round(1) : nil
        }
      }
    end

    # Per-step accumulator: task-state counts plus running sums/maxes for the
    # three resource metrics (realtime, %cpu, peak_rss), each with its own sample
    # count so averages only divide by tasks that actually reported the metric.
    def new_proc(name)
      { name: name, total: 0, done: 0, cached: 0, running: 0, pending: 0, failed: 0, other: 0,
        rt_sum: 0.0, rt_max: 0.0, rt_n: 0, cpu_sum: 0.0, cpu_max: 0.0, cpu_n: 0,
        rss_sum: 0.0, rss_max: 0.0, rss_n: 0, vmem_max: 0.0,
        rchar_sum: 0.0, wchar_sum: 0.0, t_start: nil, t_end: nil }
    end

    def tally(p, bucket, m)
      p[:total] += 1
      p[bucket] += 1
      if m[:rt].positive?
        p[:rt_sum] += m[:rt]; p[:rt_n] += 1
        p[:rt_max] = m[:rt] if m[:rt] > p[:rt_max]
      end
      if m[:cpu]
        p[:cpu_sum] += m[:cpu]; p[:cpu_n] += 1
        p[:cpu_max] = m[:cpu] if m[:cpu] > p[:cpu_max]
      end
      if m[:rss].positive?
        p[:rss_sum] += m[:rss]; p[:rss_n] += 1
        p[:rss_max] = m[:rss] if m[:rss] > p[:rss_max]
      end
      p[:vmem_max] = m[:vmem] if m[:vmem] > p[:vmem_max]
      p[:rchar_sum] += m[:rchar]
      p[:wchar_sum] += m[:wchar]
    end

    # Collapse an accumulator into the JSON shape the UI consumes.
    def finalize_proc(p)
      {
        name: p[:name], total: p[:total], done: p[:done], cached: p[:cached],
        running: p[:running], pending: p[:pending], failed: p[:failed],
        time_avg_s: p[:rt_n].positive? ? (p[:rt_sum] / p[:rt_n]).round : 0,
        time_max_s: p[:rt_max].round,
        time_total_s: p[:rt_sum].round,
        cpu_avg: p[:cpu_n].positive? ? (p[:cpu_sum] / p[:cpu_n]).round(1) : nil,
        cpu_max: p[:cpu_n].positive? ? p[:cpu_max].round(1) : nil,
        mem_avg_mb: p[:rss_n].positive? ? (p[:rss_sum] / p[:rss_n]).round : 0,
        mem_max_mb: p[:rss_max].round,
        vmem_max_mb: p[:vmem_max].round,
        io_read_mb: p[:rchar_sum].round,
        io_write_mb: p[:wchar_sum].round
      }
    end

    # "2026-06-25 13:38:45.151" -> epoch float, or nil.
    def parse_time(text)
      s = text.to_s.strip
      return nil if s.empty? || s == "-"

      Time.parse(s).to_f
    rescue ArgumentError, TypeError
      nil
    end

    def bucket_for(status)
      s = status.to_s
      return :done if s == "COMPLETED"
      return :cached if s == "CACHED"
      return :running if s == "RUNNING"
      return :pending if PENDING_STATES.include?(s)
      return :failed if FAIL_STATES.include?(s)

      :other
    end

    def base_process_name(name)
      # "NFCORE:...:GUNZIP_GTF (Mus_musculus...)" -> drop the per-task "(...)" tag,
      # then keep the final module name after the last ":".
      base = name.to_s.sub(/\s*\([^)]*\)\s*\z/, "")
      base.include?(":") ? base.split(":").last : base
    end

    def empty_counts
      { total: 0, done: 0, cached: 0, running: 0, pending: 0, failed: 0, other: 0 }
    end

    def empty_resources
      { realtime_sum_s: 0, cpu_avg: nil, peak_rss_mb: 0 }
    end

    # --- Reports + log -------------------------------------------------------

    # Which nf-core reports exist for this run. HTML reports are flagged as
    # available (true); the frontend links them to the app's own serve_report
    # route so they render as HTML instead of raw source. The output directory
    # still links to the OnDemand file explorer for browsing/downloading.
    def report_links(pipeline_info, outdir, created_at)
      links = {}
      REPORT_GLOBS.each do |key, glob|
        files = Dir.glob(File.join(pipeline_info, glob))
        links[key.to_sym] = true if files.any? { |f| report_current?(f, created_at) }
      end
      mqc = multiqc_file(outdir)
      links[:multiqc] = true if mqc && report_current?(mqc, created_at)
      links[:dir] = files_url(pipeline_info) if File.directory?(pipeline_info)
      links
    end

    # Link to the OnDemand file explorer (for directories / browsing).
    def files_url(abspath)
      "/pun/sys/dashboard/files/fs#{abspath}"
    end

    # Last `max_lines` lines of a possibly-large log, reading only the tail.
    def tail_file(path, max_lines = LOG_TAIL_LINES, max_bytes = LOG_TAIL_BYTES)
      return "" unless path && File.file?(path)

      size = File.size(path)
      data = File.open(path, "rb") do |f|
        f.seek(-[max_bytes, size].min, IO::SEEK_END) if size > max_bytes
        f.read
      end
      lines = data.to_s.split("\n")
      lines.shift if size > max_bytes && lines.length > 1 # drop partial first line
      lines.last(max_lines).join("\n")
    rescue StandardError
      ""
    end

    # Parse Nextflow's own progress from output.log:
    #   * the live status board — one line per process, "… | <done> of <total>",
    #     where <total> is the real per-step task count Nextflow expects (this is
    #     what the trace file can't tell us until tasks are actually submitted); and
    #   * the completion summary block (Succeeded/Failed/CPU hours/Duration).
    # We read only the tail, and parse the LAST status board (the most recent).
    def parse_progress(log_path)
      empty = { finished: false, outcome: nil, board_total: nil, board_done: nil,
                waiting: 0, succeeded: nil, failed: nil, ignored: nil,
                cpu_hours: nil, nf_duration: nil }
      text = tail_file(log_path, 600, 256 * 1024)
      return empty if text.empty?

      lines = text.lines
      board_re = /\A\[\w{2}\/\w+\].*\|\s*(\d+) of (\d+)/
      idxs = lines.each_index.select { |i| lines[i] =~ board_re }

      board_total = board_done = nil
      waiting = 0
      unless idxs.empty?
        last = idxs.max
        start = last
        # Walk up through the contiguous final board (board lines + "Plus N more").
        start -= 1 while start.positive? &&
                         (lines[start - 1] =~ board_re || lines[start - 1] =~ /Plus \d+ more process/)
        board_total = 0
        board_done = 0
        lines[start..last].each do |ln|
          if (m = ln.match(board_re))
            board_done += m[1].to_i
            board_total += m[2].to_i
          elsif (m = ln.match(/Plus (\d+) more process/))
            waiting += m[1].to_i
          end
        end
      end

      outcome =
        if text =~ /Pipeline completed successfully/ then "success"
        elsif text =~ /completed with err/i then "error"
        elsif text =~ /Execution cancelled|Pipeline .*cancelled/i then "cancelled"
        end

      grab_i = ->(re) { (m = text.match(re)) ? m[1].to_i : nil }
      succeeded = grab_i.call(/^\s*Succeeded\s*:\s*(\d+)/)
      failed    = grab_i.call(/^\s*Failed\s*:\s*(\d+)/)
      ignored   = grab_i.call(/^\s*Ignored\s*:\s*(\d+)/)
      cpu_hours = (m = text.match(/CPU hours\s*:\s*([\d.]+)/)) ? m[1].to_f : nil
      nf_dur    = (m = text.match(/^\s*Duration\s*:\s*(.+?)\s*$/)) ? m[1] : nil

      {
        finished: !outcome.nil? || !succeeded.nil?,
        outcome: outcome, board_total: board_total, board_done: board_done, waiting: waiting,
        succeeded: succeeded, failed: failed, ignored: ignored,
        cpu_hours: cpu_hours, nf_duration: nf_dur
      }
    end

    # Turn task-state counts + Nextflow's own progress into an honest completion
    # figure. The trace only ever reports tasks it has *seen*, so on a fresh run
    # "seen" is a useless denominator (2 of 2 = 100%). We prefer, in order:
    #   finished  -> Nextflow's Succeeded+Failed(+Ignored) summary (authoritative)
    #   running   -> the status board's total (real per-step expected counts)
    #   fallback  -> tasks seen in the trace
    # and we never report 100% while the run is still going.
    def progress_stats(counts, prog, running)
      completed = counts[:done].to_i + counts[:cached].to_i
      seen = counts[:total].to_i

      if prog[:finished] && prog[:succeeded]
        total = prog[:succeeded].to_i + prog[:failed].to_i + prog[:ignored].to_i
      elsif running && prog[:board_total]
        total = prog[:board_total].to_i
      else
        total = seen
      end
      total = [total, completed, seen].max

      percent =
        if total.zero? then nil
        elsif running then [(100.0 * completed / total).floor, 99].min
        else (100.0 * completed / total).round
        end

      { completed: completed, total: total, percent: percent,
        estimate: running, waiting: prog[:waiting].to_i }
    end

    # --- small parsers -------------------------------------------------------

    # "nf-core-rnaseq-3-25-0" -> "nf-core/rnaseq 3.25.0"
    def pipeline_label(app)
      s = app.to_s.sub(/\Anf-core-/, "")
      m = s.match(/\A(.*?)-(\d+(?:-\d+)*)\z/)
      return "nf-core/#{s}" unless m

      "nf-core/#{m[1]} #{m[2].tr('-', '.')}"
    end

    # Best-effort run start time when the db record has aged out.
    def dir_created_at(dir)
      conn = File.join(dir, "connection.yml")
      (File.file?(conn) ? File.mtime(conn) : File.mtime(dir)).to_i
    rescue StandardError
      0
    end

    def load_yaml(path)
      return {} unless File.file?(path)

      YAML.safe_load(File.read(path)) || {}
    rescue StandardError
      {}
    end

    def load_json(path)
      return {} unless File.file?(path)

      JSON.parse(File.read(path))
    rescue StandardError
      {}
    end

    def presence(value)
      s = value.to_s.strip
      s.empty? ? nil : s
    end

    # "4.2s", "8m 23s", "1h 2m 3s", "1d 2h" -> seconds
    def parse_duration(text)
      s = text.to_s.strip
      return 0.0 if s.empty? || s == "-"

      total = 0.0
      s.scan(/([\d.]+)\s*(ms|d|h|m|s)/) do |num, unit|
        n = num.to_f
        total += case unit
                 when "ms" then n / 1000.0
                 when "s" then n
                 when "m" then n * 60
                 when "h" then n * 3600
                 when "d" then n * 86_400
                 else 0
                 end
      end
      total
    end

    # "674.9 MB", "1.4 GB", "8.1 KB" -> megabytes
    def parse_size_mb(text)
      s = text.to_s.strip
      return 0.0 if s.empty? || s == "-"

      num = s[/[\d.]+/].to_f
      unit = s[/[KMGT]?B/i].to_s.upcase
      case unit
      when "B" then num / (1024.0 * 1024)
      when "KB" then num / 1024.0
      when "MB" then num
      when "GB" then num * 1024
      when "TB" then num * 1024 * 1024
      else num
      end
    end

    def parse_percent(text)
      s = text.to_s.strip.sub("%", "")
      return nil if s.empty? || s == "-"

      Float(s)
    rescue ArgumentError
      nil
    end
  end

  # --- Routes ----------------------------------------------------------------
  helpers do
    def app_root
      request.path.to_s.sub(%r{/\z}, "")
    end
  end

  def runs_json
    content_type :json
    { runs: self.class.list_runs }.to_json
  end

  def run_json(id)
    content_type :json
    halt 400, { error: "Invalid run id." }.to_json unless id.to_s.match?(UUID_PATTERN)

    detail = self.class.run_detail(id)
    halt 404, { error: "Run not found." }.to_json unless detail
    detail.to_json
  end

  get("/") { index_page }
  get("/pun/:env/nf_monitor/?") { index_page }

  get("/api/runs") { runs_json }
  get("/pun/:env/nf_monitor/api/runs") { runs_json }

  get("/api/runs/:id") { run_json(params[:id]) }
  get("/pun/:env/nf_monitor/api/runs/:id") { run_json(params[:id]) }

  get("/report/:id/:key") { serve_report(params[:id], params[:key]) }
  get("/pun/:env/nf_monitor/report/:id/:key") { serve_report(params[:id], params[:key]) }

  private

  # Serve an nf-core HTML report inline with a real text/html content-type so the
  # browser renders it. OnDemand's own Files app deliberately serves user HTML as
  # text/plain (anti-XSS), which is why those links show raw source. The path is
  # resolved from the user's OnDemand records via report_file — request input is
  # limited to a validated run id and a key from REPORT_KEYS.
  def serve_report(id, key)
    halt 400, "Invalid run id." unless id.to_s.match?(UUID_PATTERN)
    halt 404, "Unknown report." unless REPORT_KEYS.include?(key)

    path = self.class.send(:report_file, id, key)
    halt 404, "Report not found." unless path && File.file?(path)

    send_file path, type: "text/html", disposition: "inline"
  end

  def index_page
    <<~HTML
      <!DOCTYPE html>
      <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Nextflow Pipeline Monitor</title>
          <style>#{page_css}</style>
        </head>
        <body>
          <div class="app">
            <header class="topbar">
              <div class="brand">
                <span class="logo" aria-hidden="true">
                  <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M5 4c0 6 14 10 14 16"></path><path d="M19 4c0 6-14 10-14 16"></path><path d="M7.5 7h9M7.5 17h9M6 12h12"></path></svg>
                </span>
                <div class="brand-text">
                  <div class="title">Nextflow Pipeline Monitor</div>
                  <div class="subtitle">nf-core runs on Open OnDemand</div>
                </div>
              </div>
              <div class="topbar-actions">
                <button id="back" class="btn ghost" type="button" hidden>&larr; All runs</button>
                <span id="live" class="live" hidden><i></i>LIVE</span>
                <label for="interval" class="reflabel">Auto-refresh</label>
                <select id="interval" class="select">
                  <option value="10000">10s</option>
                  <option value="15000" selected>15s</option>
                  <option value="30000">30s</option>
                  <option value="0">Off</option>
                </select>
                <span id="updated" class="updated"></span>
              </div>
            </header>

            <main class="content">
              <div id="status" class="status" hidden></div>

              <section id="list" class="view">
                <div id="summary" class="summary"></div>
                <div class="toolbar">
                  <input id="search" class="search" type="search" placeholder="Filter by pipeline or run name…" autocomplete="off">
                  <span id="count" class="count"></span>
                </div>
                <div class="key-row" aria-label="Progress bar colour key">
                  <span class="keyi"><i class="sw ok"></i>Completed</span>
                  <span class="keyi"><i class="sw run"></i>Running</span>
                  <span class="keyi"><i class="sw pend"></i>Pending</span>
                  <span class="keyi"><i class="sw fail"></i>Failed</span>
                  <span class="keyi"><i class="sw cached"></i>Cached</span>
                  <span class="keyi"><i class="sw track"></i>Not started</span>
                </div>
                <div id="runs" class="runs"></div>
              </section>

              <section id="detail" class="view" hidden></section>
            </main>
          </div>
          <script>#{page_js}</script>
        </body>
      </html>
    HTML
  end

  def page_css
    <<~CSS
      :root {
        --bg:#f3f6f4; --bg2:#eaf0ec; --panel:#ffffff; --panel2:#f7faf8;
        --text:#14231b; --muted:#5c6b62; --faint:#8b978f; --border:#dde5df; --border2:#eef2ef;
        --accent:#12805c; --accent-ink:#0c5e43; --accent-soft:#e3f2ec;
        --ok:#1f9d57; --run:#2b7fd4; --fail:#d64545; --wait:#c98a1a; --cached:#8b7fd4; --idle:#8b978f;
        --ok-soft:#e4f4ea; --run-soft:#e4eefb; --fail-soft:#fbe6e6; --wait-soft:#faf0dd; --cached-soft:#ede9fb;
        --track:#e9efeb; --shadow:0 1px 2px rgba(15,45,30,.04),0 8px 24px rgba(15,45,30,.06);
        --radius:14px; --radius-sm:10px; --term-bg:#0d1f17; --term-text:#cfe8d8;
        --mono:ui-monospace,"SF Mono",SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;
        --sans:system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
      }
      @media (prefers-color-scheme: dark) {
        :root {
          --bg:#0f1613; --bg2:#0b110e; --panel:#16211c; --panel2:#131d18;
          --text:#e6efe9; --muted:#9db1a5; --faint:#6f8479; --border:#26332c; --border2:#1e2823;
          --accent:#2bb888; --accent-ink:#8fe3c5; --accent-soft:#123028;
          --ok:#35c274; --run:#5aa0ec; --fail:#f0736f; --wait:#e0aa4a; --cached:#a99cf0; --idle:#7f9186;
          --ok-soft:#12301f; --run-soft:#13233a; --fail-soft:#341a1a; --wait-soft:#312712; --cached-soft:#211d3a;
          --track:#22302a; --shadow:0 1px 2px rgba(0,0,0,.3),0 10px 28px rgba(0,0,0,.35);
          --term-bg:#0a1510; --term-text:#bfe0cd;
        }
      }
      :root[data-theme="light"] {
        --bg:#f3f6f4; --bg2:#eaf0ec; --panel:#ffffff; --panel2:#f7faf8;
        --text:#14231b; --muted:#5c6b62; --faint:#8b978f; --border:#dde5df; --border2:#eef2ef;
        --accent:#12805c; --accent-ink:#0c5e43; --accent-soft:#e3f2ec; --track:#e9efeb;
        --ok:#1f9d57; --run:#2b7fd4; --fail:#d64545; --wait:#c98a1a; --cached:#8b7fd4; --idle:#8b978f;
        --ok-soft:#e4f4ea; --run-soft:#e4eefb; --fail-soft:#fbe6e6; --wait-soft:#faf0dd; --cached-soft:#ede9fb;
        --term-bg:#0d1f17; --term-text:#cfe8d8;
      }
      :root[data-theme="dark"] {
        --bg:#0f1613; --bg2:#0b110e; --panel:#16211c; --panel2:#131d18;
        --text:#e6efe9; --muted:#9db1a5; --faint:#6f8479; --border:#26332c; --border2:#1e2823;
        --accent:#2bb888; --accent-ink:#8fe3c5; --accent-soft:#123028; --track:#22302a;
        --ok:#35c274; --run:#5aa0ec; --fail:#f0736f; --wait:#e0aa4a; --cached:#a99cf0; --idle:#7f9186;
        --ok-soft:#12301f; --run-soft:#13233a; --fail-soft:#341a1a; --wait-soft:#312712; --cached-soft:#211d3a;
        --term-bg:#0a1510; --term-text:#bfe0cd;
      }

      * { box-sizing:border-box; }
      html { color-scheme:light dark; }
      body { margin:0; background:var(--bg); color:var(--text); font-family:var(--sans);
        font-size:15px; line-height:1.5; -webkit-font-smoothing:antialiased; }
      .app { min-height:100vh; }
      a { color:var(--accent-ink); text-decoration:none; }
      a:hover { text-decoration:underline; }
      button { font:inherit; }
      :focus-visible { outline:2px solid var(--accent); outline-offset:2px; border-radius:6px; }

      .topbar { position:sticky; top:0; z-index:10; display:flex; align-items:center;
        justify-content:space-between; gap:1rem; flex-wrap:wrap;
        padding:.85rem 1.5rem; background:color-mix(in srgb, var(--panel) 88%, transparent);
        backdrop-filter:saturate(1.4) blur(8px); border-bottom:1px solid var(--border); }
      .brand { display:flex; align-items:center; gap:.75rem; }
      .logo { display:grid; place-items:center; width:38px; height:38px; border-radius:11px;
        color:#fff; background:linear-gradient(145deg,var(--accent),var(--accent-ink));
        box-shadow:0 4px 12px color-mix(in srgb,var(--accent) 40%,transparent); }
      .title { font-weight:700; font-size:1.02rem; letter-spacing:-.01em; }
      .subtitle { color:var(--muted); font-size:.8rem; }
      .topbar-actions { display:flex; align-items:center; gap:.6rem; flex-wrap:wrap; }
      .reflabel { color:var(--muted); font-size:.82rem; font-weight:600; }
      .updated { color:var(--faint); font-size:.78rem; min-width:8.5rem; text-align:right;
        font-variant-numeric:tabular-nums; }
      .select { padding:.42rem .6rem; border:1px solid var(--border); border-radius:9px;
        background:var(--panel); color:var(--text); font-size:.85rem; cursor:pointer; }
      .btn { padding:.5rem .9rem; border-radius:9px; border:1px solid var(--accent);
        background:var(--accent); color:#fff; font-weight:600; cursor:pointer; font-size:.85rem; }
      .btn:hover { filter:brightness(1.05); }
      .btn.ghost { background:var(--panel); color:var(--accent-ink); border-color:var(--border); }
      .btn.ghost:hover { border-color:var(--accent); }
      .live { display:inline-flex; align-items:center; gap:.4rem; color:var(--run);
        font-weight:700; font-size:.72rem; letter-spacing:.08em; text-transform:uppercase; }
      .live i { width:8px; height:8px; border-radius:50%; background:var(--run);
        box-shadow:0 0 0 0 color-mix(in srgb,var(--run) 60%,transparent); animation:pulse 1.8s infinite; }
      @keyframes pulse { 0%{box-shadow:0 0 0 0 color-mix(in srgb,var(--run) 55%,transparent);}
        70%{box-shadow:0 0 0 7px transparent;} 100%{box-shadow:0 0 0 0 transparent;} }
      @media (prefers-reduced-motion: reduce) { .live i, .dot.run::after { animation:none; } }

      .content { max-width:78rem; margin:0 auto; padding:1.5rem; }
      .status { margin-bottom:1rem; padding:.85rem 1rem; border-radius:var(--radius-sm); border:1px solid transparent; font-size:.9rem; }
      .status.warning { background:var(--wait-soft); color:var(--wait); border-color:color-mix(in srgb,var(--wait) 35%,transparent); }
      .status.error { background:var(--fail-soft); color:var(--fail); border-color:color-mix(in srgb,var(--fail) 35%,transparent); }

      /* summary chips */
      .summary { display:grid; grid-template-columns:repeat(auto-fit,minmax(9rem,1fr)); gap:.75rem; margin-bottom:1.1rem; }
      .chip { text-align:left; padding:.85rem 1rem; border:1px solid var(--border); border-radius:var(--radius);
        background:var(--panel); cursor:pointer; box-shadow:var(--shadow); position:relative; overflow:hidden;
        transition:transform .12s ease, border-color .12s ease; }
      .chip:hover { transform:translateY(-1px); }
      .chip[aria-pressed="true"] { border-color:var(--accent); box-shadow:0 0 0 1px var(--accent) inset, var(--shadow); }
      .chip .n { font-size:1.7rem; font-weight:700; letter-spacing:-.02em; font-variant-numeric:tabular-nums; }
      .chip .l { color:var(--muted); font-size:.78rem; font-weight:600; text-transform:uppercase; letter-spacing:.04em; }
      .chip.total .n { color:var(--accent-ink); }
      .chip.running .n { color:var(--run); }
      .chip.completed .n { color:var(--ok); }
      .chip.failed .n { color:var(--fail); }
      .chip .spark { position:absolute; right:.7rem; top:.7rem; width:10px; height:10px; border-radius:50%; }
      .chip.running .spark { background:var(--run); }
      .chip.completed .spark { background:var(--ok); }
      .chip.failed .spark { background:var(--fail); }
      .chip.total .spark { background:var(--accent); }

      .toolbar { display:flex; align-items:center; gap:.75rem; margin-bottom:.9rem; }
      .search { flex:1; padding:.6rem .85rem; border:1px solid var(--border); border-radius:10px;
        background:var(--panel); color:var(--text); font-size:.9rem; }
      .count { color:var(--faint); font-size:.8rem; white-space:nowrap; }

      /* run list */
      .runs { display:flex; flex-direction:column; gap:.6rem; }
      .run { display:grid; grid-template-columns:minmax(13rem,1.4fr) minmax(12rem,1.6fr) auto;
        gap:1rem; align-items:center; padding:.9rem 1.1rem; background:var(--panel);
        border:1px solid var(--border); border-radius:var(--radius); box-shadow:var(--shadow);
        cursor:pointer; transition:transform .12s ease, border-color .12s ease; text-align:left; width:100%; }
      .run:hover { transform:translateY(-1px); border-color:color-mix(in srgb,var(--accent) 45%,var(--border)); }
      .run.is-running { border-left:3px solid var(--run); }
      .run-id { display:flex; align-items:center; gap:.6rem; min-width:0; }
      .dot { width:9px; height:9px; border-radius:50%; flex:none; position:relative; }
      .dot.ok{background:var(--ok);} .dot.run{background:var(--run);} .dot.wait{background:var(--wait);}
      .dot.fail{background:var(--fail);} .dot.idle{background:var(--idle);}
      .dot.run::after { content:""; position:absolute; inset:-4px; border-radius:50%;
        border:2px solid var(--run); opacity:.5; animation:ring 1.8s infinite; }
      @keyframes ring { 0%{transform:scale(.6);opacity:.6;} 100%{transform:scale(1.6);opacity:0;} }
      .run-name { min-width:0; }
      .run-name .p { font-weight:650; letter-spacing:-.01em; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
      .run-name .r { color:var(--muted); font-size:.8rem; font-family:var(--mono); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
      .ver { display:inline-block; margin-left:.4rem; padding:.05rem .4rem; border-radius:6px; font-size:.7rem;
        font-weight:700; color:var(--accent-ink); background:var(--accent-soft); font-variant-numeric:tabular-nums; }
      .run-meta { display:flex; align-items:center; gap:1rem; justify-content:flex-end; white-space:nowrap; }
      .run-meta .m { text-align:right; }
      .run-meta .m .v { font-weight:600; font-size:.85rem; font-variant-numeric:tabular-nums; }
      .run-meta .m .k { color:var(--faint); font-size:.7rem; text-transform:uppercase; letter-spacing:.03em; }

      /* progress bar */
      .prog { min-width:11rem; }
      .prog .top { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:.35rem; }
      .prog .pct { font-weight:700; font-size:.9rem; font-variant-numeric:tabular-nums; }
      .prog .frac { color:var(--faint); font-size:.75rem; font-variant-numeric:tabular-nums; }
      .bar { height:.55rem; border-radius:999px; background:var(--track); overflow:hidden; display:flex; }
      .bar > i { height:100%; display:block; transition:width .5s ease; }
      .bar > i { cursor:help; }
      .bar > i.ok{background:var(--ok);} .bar > i.cached{background:var(--cached);}
      .bar > i.run{background:var(--run);} .bar > i.pend{background:var(--wait);} .bar > i.fail{background:var(--fail);}
      /* tasks left unfinished when a run ended (killed/cancelled/timed out): neutral, not "live". */
      .bar > i.stale{ background:var(--idle); opacity:.55; }
      /* Running segment gets moving stripes so it reads as "actively working". */
      .bar > i.run { background-image:linear-gradient(45deg, rgba(255,255,255,.35) 25%, transparent 25%,
        transparent 50%, rgba(255,255,255,.35) 50%, rgba(255,255,255,.35) 75%, transparent 75%, transparent);
        background-size:.55rem .55rem; animation:stripe 1s linear infinite; }
      @keyframes stripe { from{background-position:0 0;} to{background-position:.55rem 0;} }
      @media (prefers-reduced-motion: reduce){ .bar > i.run { animation:none; } }
      .legend { display:flex; gap:.9rem; flex-wrap:wrap; margin-top:.5rem; color:var(--muted); font-size:.76rem; }
      .legend b { color:var(--text); font-variant-numeric:tabular-nums; }
      .legend .sw { display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:.35rem; vertical-align:middle; }
      /* colour key */
      .sw { display:inline-block; width:10px; height:10px; border-radius:2px; }
      .sw.ok{background:var(--ok);} .sw.run{background:var(--run);} .sw.pend{background:var(--wait);}
      .sw.fail{background:var(--fail);} .sw.cached{background:var(--cached);} .sw.track{background:var(--track); border:1px solid var(--border);}
      .sw.stale{background:var(--idle); opacity:.55;}
      .key-row { display:flex; gap:1rem; flex-wrap:wrap; align-items:center; margin-bottom:.9rem;
        color:var(--muted); font-size:.76rem; }
      .keyi { display:inline-flex; align-items:center; gap:.35rem; }

      /* pills */
      .pill { display:inline-flex; align-items:center; gap:.3rem; padding:.18rem .55rem; border-radius:999px;
        font-size:.72rem; font-weight:700; letter-spacing:.02em; text-transform:uppercase; }
      .pill.ok{background:var(--ok-soft);color:var(--ok);} .pill.run{background:var(--run-soft);color:var(--run);}
      .pill.wait{background:var(--wait-soft);color:var(--wait);} .pill.fail{background:var(--fail-soft);color:var(--fail);}
      .pill.cached{background:var(--cached-soft);color:var(--cached);} .pill.idle{background:var(--track);color:var(--muted);}

      /* detail */
      .card { background:var(--panel); border:1px solid var(--border); border-radius:var(--radius);
        box-shadow:var(--shadow); padding:1.3rem 1.4rem; margin-bottom:1.1rem; }
      .card h3 { margin:0 0 .9rem; font-size:.82rem; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); }
      .head { display:flex; justify-content:space-between; align-items:flex-start; gap:1.5rem; flex-wrap:wrap; }
      .head .h-main h2 { margin:0 0 .3rem; font-size:1.5rem; letter-spacing:-.02em; }
      .head .h-main .run-line { color:var(--muted); font-family:var(--mono); font-size:.85rem; }
      .head .h-main .badges { display:flex; align-items:center; gap:.5rem; margin-top:.6rem; flex-wrap:wrap; }

      /* progress ring */
      .ring { --p:0; width:104px; height:104px; border-radius:50%; flex:none; display:grid; place-items:center;
        background:conic-gradient(var(--ok) calc(var(--p)*1%), var(--track) 0); }
      .ring::before { content:""; position:absolute; width:78px; height:78px; border-radius:50%; background:var(--panel); }
      .ring-wrap { position:relative; display:grid; place-items:center; }
      .ring .inner { position:relative; text-align:center; }
      .ring .inner .v { font-size:1.35rem; font-weight:700; font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
      .ring .inner .k { font-size:.65rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; }

      .facts { display:grid; grid-template-columns:repeat(auto-fit,minmax(12rem,1fr)); gap:.9rem 1.5rem; margin-top:1.1rem;
        border-top:1px solid var(--border2); padding-top:1.1rem; }
      .facts .k { color:var(--faint); font-size:.7rem; text-transform:uppercase; letter-spacing:.04em; margin-bottom:.15rem; }
      .facts .v { font-size:.9rem; word-break:break-word; }
      .facts .v.mono { font-family:var(--mono); font-size:.82rem; }

      .reports { display:flex; gap:.55rem; flex-wrap:wrap; margin-top:1.1rem; }
      .reports .btn.ghost { display:inline-flex; align-items:center; gap:.4rem; }

      /* stat tiles */
      .tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(8rem,1fr)); gap:.75rem; }
      .tile { border:1px solid var(--border); border-radius:var(--radius-sm); padding:.85rem .95rem; background:var(--panel2); }
      .tile .n { font-size:1.55rem; font-weight:700; letter-spacing:-.02em; font-variant-numeric:tabular-nums; }
      .tile .l { color:var(--muted); font-size:.75rem; margin-top:.1rem; }
      .tile.ok .n{color:var(--ok);} .tile.run .n{color:var(--run);} .tile.fail .n{color:var(--fail);} .tile.cached .n{color:var(--cached);}

      /* data tables */
      .scroll { max-height:28rem; overflow:auto; border:1px solid var(--border); border-radius:var(--radius-sm); }
      table.data { width:100%; border-collapse:collapse; font-size:.83rem; }
      table.data th, table.data td { text-align:left; padding:.5rem .7rem; border-top:1px solid var(--border2); vertical-align:middle; }
      table.data thead th { position:sticky; top:0; z-index:1; background:var(--panel2); color:var(--muted);
        font-weight:600; font-size:.72rem; text-transform:uppercase; letter-spacing:.03em; border-top:none; }
      table.data tbody tr:hover td { background:var(--panel2); }
      table.data td.num, table.data th.num { text-align:right; font-variant-numeric:tabular-nums; }
      .proc-name { font-family:var(--mono); font-size:.8rem; }
      .pbar { display:flex; align-items:center; gap:.55rem; min-width:7rem; }
      .pbar .bar { flex:1; min-width:4rem; height:.4rem; }
      .pbar .frac { color:var(--faint); font-size:.72rem; font-variant-numeric:tabular-nums; white-space:nowrap; }
      /* per-step numeric cells: emphasized value + muted secondary */
      td.stat b { font-weight:650; font-variant-numeric:tabular-nums; }
      td.stat .mut { color:var(--faint); font-size:.76rem; }
      table.data td.cnt { text-align:right; font-variant-numeric:tabular-nums; font-weight:600; }
      table.data td.cnt.z { color:var(--faint); font-weight:400; }
      table.data td.cnt.run{color:var(--run);} table.data td.cnt.pend{color:var(--wait);} table.data td.cnt.fail{color:var(--fail);}
      table.steps { min-width:52rem; }
      .card-head { display:flex; align-items:center; justify-content:space-between; gap:1rem; margin-bottom:.9rem; }
      .card-head h3 { margin:0; }
      .card-head .select { padding:.35rem .55rem; font-size:.8rem; }

      pre.log { margin:0; background:var(--term-bg); color:var(--term-text); padding:1rem 1.1rem;
        border-radius:var(--radius-sm); overflow:auto; max-height:26rem; font-family:var(--mono);
        font-size:.79rem; line-height:1.55; white-space:pre-wrap; word-break:break-word; }
      pre.cmd { margin:0; background:var(--panel2); color:var(--text); border:1px solid var(--border);
        padding:.8rem 1rem; border-radius:var(--radius-sm); overflow:auto; font-family:var(--mono);
        font-size:.8rem; line-height:1.5; white-space:pre-wrap; word-break:break-word; }
      /* execution timeline (gantt) */
      .gantt { display:flex; flex-direction:column; gap:.3rem; max-height:30rem; overflow:auto; }
      .grow { display:grid; grid-template-columns:14rem 1fr 4rem; align-items:center; gap:.6rem; }
      .glabel { font-family:var(--mono); font-size:.76rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
      .gtrack { position:relative; height:.85rem; background:var(--track); border-radius:4px; }
      .gbar { position:absolute; top:0; height:100%; border-radius:4px; min-width:2px; }
      .gbar.ok { background:linear-gradient(90deg,var(--accent),var(--ok)); }
      .gbar.fail { background:linear-gradient(90deg,var(--fail),#e88); }
      .gdur { font-size:.72rem; color:var(--faint); text-align:right; font-variant-numeric:tabular-nums; }
      .gaxis { display:flex; justify-content:space-between; margin:.5rem 0 0 14.6rem; color:var(--faint);
        font-size:.72rem; font-variant-numeric:tabular-nums; border-top:1px solid var(--border2); padding-top:.3rem; }
      @media (max-width:760px){ .grow { grid-template-columns:7rem 1fr; } .gdur { display:none; } .gaxis { margin-left:7.6rem; } }
      td.pval { font-family:var(--mono); font-size:.8rem; word-break:break-word; }
      td.mono-cell { font-family:var(--mono); font-size:.8rem; font-variant-numeric:tabular-nums; }
      table.params { min-width:auto; }
      .empty { color:var(--faint); font-size:.85rem; }
      .note { color:var(--muted); font-size:.78rem; margin:.7rem 0 0; line-height:1.45; }
      .skeleton { height:5rem; border-radius:var(--radius); background:linear-gradient(90deg,var(--panel) 25%,var(--panel2) 50%,var(--panel) 75%);
        background-size:200% 100%; animation:sk 1.3s infinite; margin-bottom:.6rem; border:1px solid var(--border); }
      @keyframes sk { 0%{background-position:200% 0;} 100%{background-position:-200% 0;} }

      @media (max-width:760px) {
        .content { padding:1rem; }
        .run { grid-template-columns:1fr; gap:.7rem; }
        .run-meta { justify-content:flex-start; }
        .head { flex-direction:column; }
      }
    CSS
  end

  def page_js
    <<~JS
      (function () {
        "use strict";
        const BASE = #{app_root.to_json};
        const $ = (id) => document.getElementById(id);
        const listEl = $("list"), detailEl = $("detail"), statusEl = $("status");
        const runsEl = $("runs"), summaryEl = $("summary"), searchEl = $("search"), countEl = $("count");
        const backEl = $("back"), liveEl = $("live"), intervalSel = $("interval"), updatedEl = $("updated");

        let timer = null, current = null, filter = "all", runsCache = [];
        let lastProcs = [], procSort = "name", lastParams = [], lastRunning = false;

        const api = (p) => fetch(BASE + p, {headers:{Accept:"application/json"}}).then(r => r.json());
        const esc = (s) => { const d = document.createElement("div"); d.textContent = s==null?"":String(s); return d.innerHTML; };
        const showStatus = (k,m) => { statusEl.className = "status "+k; statusEl.textContent = m; statusEl.hidden = false; };
        const clearStatus = () => { statusEl.hidden = true; };
        const stamp = () => { updatedEl.textContent = "Updated " + new Date().toLocaleTimeString(); };

        function relTime(sec){
          if (!sec) return "—";
          const d = Math.floor(Date.now()/1000) - sec;
          if (d < 60) return "just now";
          if (d < 3600) return Math.floor(d/60) + "m ago";
          if (d < 86400) return Math.floor(d/3600) + "h ago";
          if (d < 604800) return Math.floor(d/86400) + "d ago";
          return new Date(sec*1000).toLocaleDateString();
        }
        function fmtDur(s){ s = Math.round(s||0); if(!s) return "0s";
          const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), x=s%60;
          return (h?h+"h ":"")+(m||h?m+"m ":"")+x+"s"; }
        function fmtMB(mb){ mb = mb||0; if(mb>=1024) return (mb/1024).toFixed(mb>=10240?0:1)+" GB"; return Math.round(mb)+" MB"; }
        function taskK(s){ s=String(s||"").toUpperCase();
          if(s==="COMPLETED")return"ok"; if(s==="CACHED")return"cached"; if(s==="RUNNING")return"run";
          if(s==="SUBMITTED"||s==="NEW"||s==="PENDING")return"wait";
          if(s==="FAILED"||s==="ABORTED")return"fail"; return"idle"; }

        function stateVis(r){
          const s = String(r.state_class||"").toLowerCase();
          const label = r.job_state || "UNKNOWN";
          if (s==="completed") return {label, k:"ok", cat:"completed"};
          if (s==="running")   return {label, k:"run", cat:"running"};
          if (s==="pending")   return {label, k:"wait", cat:"running"};
          if (s==="failed"||s==="cancelled"||s==="timeout"||s==="node_fail"||s==="out_of_me")
            return {label, k:"fail", cat:"failed"};
          return {label, k:"idle", cat:"other"};
        }

        // opts.progress (server-computed {completed,total,percent,estimate,waiting})
        // supplies the honest denominator; the bar segments come from the raw
        // per-state counts. Any gap between (seen states) and total shows as unfilled
        // track — i.e. steps not yet started.
        function progressHTML(c, opts){
          c = c || {}; opts = opts||{};
          const pr = opts.progress || {};
          const total = pr.total || c.total || 0;
          if (!total) return '<div class="prog"><div class="bar"></div><div class="legend">No tasks recorded yet</div></div>';
          const done=c.done||0, cached=c.cached||0, failed=c.failed||0;
          let running=c.running||0, pending=c.pending||0, stale=0;
          // A finished run's leftover running/pending tasks never completed; show them
          // as neutral "unfinished", not as a live (animated blue) running segment.
          const live = opts.live !== false;
          if (!live){ stale=running+pending; running=0; pending=0; }
          const completed = (pr.completed!=null?pr.completed:done+cached);
          const pct = (pr.percent!=null ? pr.percent : Math.round(100*completed/total));
          const est = pr.estimate;
          const notStarted = Math.max(0, total-(done+cached+running+pending+failed+stale));
          const w = (n) => (100*n/total).toFixed(2)+"%";
          const sg = (cls,n,label) => n>0 ? '<i class="'+cls+'" style="width:'+w(n)+'" title="'+n+' '+label+'"></i>' : '';
          const bar = '<div class="bar">' +
            sg('ok',done,'completed') + sg('cached',cached,'cached (reused)') +
            sg('run',running,'running now') + sg('pend',pending,'pending / queued') +
            sg('fail',failed,'failed') + sg('stale',stale,'unfinished') + '</div>';
          const totLabel = total + (est?"+":"") + " tasks";
          if (opts.compact) {
            const tip = done+' completed, '+running+' running, '+pending+' pending, '+failed+' failed'+
              (stale?', '+stale+' unfinished':'')+(notStarted?', '+notStarted+' not started':'')+' of '+totLabel;
            return '<div class="prog" title="'+tip+'"><div class="top"><span class="pct">'+pct+'%'+(est?" ·":"")+'</span>' +
              '<span class="frac">'+completed+' / '+totLabel+'</span></div>'+bar+'</div>';
          }
          const seg = (cls,label,n) => n ? '<span class="keyi"><i class="sw '+cls+'"></i>'+label+' <b>'+n+'</b></span>' : '';
          const legend = '<div class="legend">' +
            seg('ok','Completed',done) + seg('cached','Cached (reused)',cached) +
            seg('run','Running now',running) + seg('pend','Pending',pending) +
            seg('fail','Failed',failed) + seg('stale','Unfinished',stale) +
            (notStarted? '<span class="keyi"><i class="sw track"></i>Not started <b>'+notStarted+'</b></span>':'') +
            (pr.waiting? '<span class="keyi">+'+pr.waiting+' steps not yet scheduled</span>':'') + '</div>';
          return '<div class="prog"><div class="top"><span class="pct">'+pct+'%'+(est?" (so far)":"")+'</span>' +
            '<span class="frac">'+completed+' / '+totLabel+'</span></div>'+bar+legend+'</div>';
        }

        // ---- summary + list ----
        function renderSummary(runs){
          const cat = {running:0, completed:0, failed:0};
          runs.forEach(r => { const c = stateVis(r).cat; if (cat[c]!=null) cat[c]++; });
          const chip = (key,label,n) =>
            '<button class="chip '+key+'" data-filter="'+key+'" aria-pressed="'+(filter===key)+'">' +
            '<span class="spark"></span><div class="n">'+n+'</div><div class="l">'+label+'</div></button>';
          summaryEl.innerHTML =
            chip("all","Total runs",runs.length) + chip("running","Running",cat.running) +
            chip("completed","Completed",cat.completed) + chip("failed","Failed",cat.failed);
          Array.prototype.forEach.call(summaryEl.querySelectorAll(".chip"), (b) =>
            b.addEventListener("click", () => { filter = b.getAttribute("data-filter"); renderList(); }));
        }

        function renderList(){
          const term = (searchEl.value||"").toLowerCase();
          const rows = runsCache.filter(r => {
            const c = stateVis(r).cat;
            if (filter==="running" && c!=="running") return false;
            if (filter==="completed" && c!=="completed") return false;
            if (filter==="failed" && c!=="failed") return false;
            if (term && !((r.pipeline||"")+" "+(r.run_name||"")).toLowerCase().includes(term)) return false;
            return true;
          });
          Array.prototype.forEach.call(summaryEl.querySelectorAll(".chip"), (b) =>
            b.setAttribute("aria-pressed", filter===b.getAttribute("data-filter")));
          countEl.textContent = rows.length + " of " + runsCache.length + " runs";
          if (!rows.length){ runsEl.innerHTML = '<p class="empty">No runs match this filter.</p>'; return; }

          runsEl.innerHTML = rows.map(r => {
            const st = stateVis(r);
            const running = st.cat==="running";
            const nm = splitName(r.pipeline);
            const dur = running ? relElapsed(r.created_at) : (r.elapsed||"—");
            return '<button class="run'+(running?" is-running":"")+'" data-id="'+esc(r.id)+'">' +
              '<span class="run-id"><span class="dot '+st.k+'"></span><span class="run-name">' +
                '<div class="p">'+esc(nm.name)+(nm.ver?'<span class="ver">'+esc(nm.ver)+'</span>':'')+'</div>' +
                '<div class="r">'+esc(r.run_name||"—")+'</div></span></span>' +
              '<span>'+progressHTML(r.counts,{compact:true, progress:r.progress, live:running})+'</span>' +
              '<span class="run-meta">' +
                '<span class="pill '+st.k+'">'+esc(st.label)+'</span>' +
                '<span class="m"><div class="v">'+esc(r.executor)+'</div><div class="k">Executor</div></span>' +
                '<span class="m"><div class="v">'+esc(dur)+'</div><div class="k">'+(running?"Elapsed":"Duration")+'</div></span>' +
                '<span class="m"><div class="v">'+esc(relTime(r.created_at))+'</div><div class="k">Started</div></span>' +
              '</span></button>';
          }).join("");
          Array.prototype.forEach.call(runsEl.querySelectorAll(".run"), (b) =>
            b.addEventListener("click", () => openDetail(b.getAttribute("data-id"))));
          liveEl.hidden = !runsCache.some(r => stateVis(r).cat==="running");
        }

        function splitName(pipeline){
          const m = String(pipeline||"").match(/^(.*?)\\s+([\\d.]+)$/);
          return m ? {name:m[1], ver:m[2]} : {name:pipeline||"—", ver:null};
        }
        function relElapsed(sec){ if(!sec) return "—"; return fmtDur(Math.floor(Date.now()/1000)-sec); }

        // ---- detail ----
        function fact(k,v,mono){ return '<div><div class="k">'+k+'</div><div class="v'+(mono?" mono":"")+'">'+esc(v||"—")+'</div></div>'; }

        function renderDetail(d){
          const st = stateVis(d), c = d.counts||{}, res = d.resources||{};
          const pr = d.progress||{};
          const total = pr.total || c.total || 0;
          const pct = (pr.percent!=null ? pr.percent : 0);
          const pctLabel = (pr.percent!=null ? pct+"%" : "—");
          const nm = splitName(d.pipeline);
          const running = st.cat==="running";
          liveEl.hidden = !running;
          lastProcs = d.processes||[];
          lastParams = d.params||[];
          lastRunning = running;

          const rep = d.reports||{}, rl = [];
          // HTML reports are served by this app (renders as HTML); dir opens the file explorer.
          const rurl = (k) => BASE + "/report/" + encodeURIComponent(current) + "/" + k;
          if (rep.multiqc)  rl.push(repBtn(rurl("multiqc"),"MultiQC report"));
          if (rep.report)   rl.push(repBtn(rurl("report"),"Execution report"));
          if (rep.timeline) rl.push(repBtn(rurl("timeline"),"Timeline"));
          if (rep.dag)      rl.push(repBtn(rurl("dag"),"DAG"));
          if (rep.dir)      rl.push(repBtn(rep.dir,"Output files"));

          const failed = d.failed_tasks||[];
          const failRows = failed.map(t => {
            const wd = workUrl(d.workdir, t.hash);
            return '<tr><td class="num">'+esc(t.task_id)+'</td><td class="proc-name">'+esc(t.name)+'</td>' +
              '<td class="num">'+esc(t.exit)+'</td>' +
              '<td>'+(wd?'<a href="'+esc(wd)+'" target="_blank" rel="noopener">work/'+esc(t.hash)+' ↗</a>':esc(t.hash||"—"))+'</td></tr>';
          }).join("");

          const taskRows = (d.recent_tasks||[]).map(t =>
            '<tr><td class="num">'+esc(t.task_id)+'</td><td class="proc-name">'+esc(t.name)+'</td>' +
            '<td><span class="pill '+taskK(t.status)+'">'+esc(t.status)+'</span></td>' +
            '<td class="num">'+esc(t.duration)+'</td><td class="num">'+esc(t.cpu)+'</td>' +
            '<td class="num">'+esc(t.peak_rss)+'</td></tr>'
          ).join("") || '<tr><td colspan="6" class="empty">No tasks recorded yet.</td></tr>';

          detailEl.innerHTML =
            '<div class="card"><div class="head">' +
              '<div class="h-main">' +
                '<h2>'+esc(nm.name)+(nm.ver?'<span class="ver">'+esc(nm.ver)+'</span>':'')+'</h2>' +
                '<div class="run-line">'+esc(d.run_name||"—")+'</div>' +
                '<div class="badges"><span class="pill '+st.k+'">'+esc(st.label)+'</span>' +
                  (running?'<span class="pill run">Live</span>':'')+'</div>' +
                '<div class="facts">' +
                  fact("Job ID", d.job_id) + fact("Scheduler state", d.job_state) +
                  fact("Executor", d.executor) +
                  fact("Nextflow", d.nextflow_version) +
                  fact("Revision", d.revision ? String(d.revision).slice(0,10) : null, true) +
                  fact("Session ID", d.session, true) +
                  fact("Started", relTime(d.created_at)+(d.created_at?" ("+new Date(d.created_at*1000).toLocaleString()+")":"")) +
                  fact("Duration", running?relElapsed(d.created_at):(d.elapsed||"—")) +
                  fact("Work dir", d.workdir, true) + fact("Output dir", d.outdir, true) +
                '</div>' +
                (rl.length?'<div class="reports">'+rl.join("")+'</div>':'') +
              '</div>' +
              '<div class="ring-wrap"><div class="ring" style="--p:'+pct+'"><div class="inner">' +
                '<div class="v">'+pctLabel+'</div><div class="k">'+(pr.estimate?"So far":"Complete")+'</div></div></div></div>' +
            '</div></div>' +

            (d.completion?completionCard(d.completion):'') +

            '<div class="card"><h3>Task progress</h3>' +
              '<div class="tiles">' +
                tile("",total+(pr.estimate?"+":""),(pr.estimate?"Tasks (so far)":"Total tasks")) +
                tile("ok",c.done||0,"Completed") + tile("run",c.running||0,"Running") +
                tile("wait",c.pending||0,"Pending") + tile("fail",c.failed||0,"Failed") + tile("cached",c.cached||0,"Cached") +
              '</div><div style="margin-top:1rem">'+progressHTML(c,{progress:pr, live:running})+'</div>' +
              (pr.estimate?'<p class="note">Nextflow discovers tasks as upstream steps finish, so the total can still grow — this is completion of what has been scheduled so far, never shown as 100% until the run ends.</p>':'') +
            '</div>' +

            (d.command?'<div class="card"><h3>Nextflow command</h3><pre class="cmd">'+esc(d.command)+'</pre></div>':'') +

            '<div class="card"><h3>Resource usage (all steps)</h3><div class="tiles">' +
              tile("",fmtDur(res.realtime_sum_s),"Total compute time") +
              tile("",(res.wall_s!=null?fmtDur(res.wall_s):"—"),"Wall-clock time") +
              tile("",(res.parallelism!=null?res.parallelism+"×":"—"),"Parallelism") +
              tile("",(res.cpu_avg!=null?res.cpu_avg+"%":"—"),"Avg %CPU / task") +
              tile("",fmtMB(res.peak_rss_mb),"Peak RSS (max task)") +
              tile("",fmtMB(res.peak_vmem_mb),"Peak virtual mem") +
              tile("",fmtMB(res.io_read_mb),"Total I/O read") +
              tile("",fmtMB(res.io_write_mb),"Total I/O written") +
            '</div></div>' +

            '<div class="card"><div class="card-head"><h3>Per-step progress &amp; resources ('+lastProcs.length+')</h3>' +
              '<select id="procsort" class="select" aria-label="Sort steps">' +
                '<option value="name">Sort: name</option>' +
                '<option value="time">Sort: total time</option>' +
                '<option value="mem">Sort: peak memory</option>' +
                '<option value="fail">Sort: failures</option>' +
                '<option value="running">Sort: running now</option>' +
                '<option value="total">Sort: task count</option>' +
              '</select></div>' +
              '<div class="scroll"><table class="data steps"><thead><tr>' +
                '<th>Step</th><th>Progress</th>' +
                '<th class="num">Run</th><th class="num">Pend</th><th class="num">Fail</th>' +
                '<th class="num">Time avg&nbsp;·&nbsp;max</th><th class="num">%CPU avg&nbsp;·&nbsp;max</th><th class="num">Mem avg&nbsp;·&nbsp;max</th>' +
              '</tr></thead><tbody id="procbody"></tbody></table></div></div>' +

            ((d.timeline||[]).length?'<div class="card"><h3>Execution timeline</h3>'+ganttHTML(d.timeline,res.wall_s)+'</div>':'') +

            (failed.length?'<div class="card"><h3>Failed tasks ('+failed.length+')</h3><div class="scroll">' +
              '<table class="data"><thead><tr><th class="num">ID</th><th>Task</th><th class="num">Exit</th><th>Work dir</th></tr></thead>' +
              '<tbody>'+failRows+'</tbody></table></div></div>':'') +

            '<div class="card"><h3>Recent tasks</h3><div class="scroll">' +
              '<table class="data"><thead><tr><th class="num">ID</th><th>Task</th><th>Status</th>' +
              '<th class="num">Duration</th><th class="num">%CPU</th><th class="num">Peak RSS</th></tr></thead>' +
              '<tbody>'+taskRows+'</tbody></table></div></div>' +

            '<div class="card"><div class="card-head"><h3>Pipeline parameters ('+(d.params||[]).length+')</h3>' +
              '<input id="paramfilter" class="search" type="search" placeholder="Filter parameters…" autocomplete="off" style="max-width:16rem">' +
              '</div><div class="scroll"><table class="data params"><thead><tr><th>Parameter</th><th>Value</th></tr></thead>' +
              '<tbody id="parambody"></tbody></table></div></div>' +

            '<div class="card"><h3>Software versions ('+(d.versions||[]).length+')</h3>' +
              ((d.versions||[]).length ?
                '<div class="scroll"><table class="data"><thead><tr><th>Tool</th><th>Version</th></tr></thead><tbody>' +
                d.versions.map(v => '<tr><td class="proc-name">'+esc(v.tool)+'</td><td class="mono-cell">'+esc(v.version)+'</td></tr>').join("") +
                '</tbody></table></div>' : '<p class="empty">Not reported yet.</p>') + '</div>' +

            '<div class="card"><h3>Log tail</h3>' +
              (d.log_tail?'<pre class="log">'+esc(d.log_tail)+'</pre>':'<p class="empty">No log output yet.</p>')+'</div>';

          const pf = $("paramfilter");
          if (pf){ pf.addEventListener("input", fillParams); }
          fillParams();

          const sortSel = $("procsort");
          if (sortSel){ sortSel.value = procSort; sortSel.addEventListener("change", () => { procSort = sortSel.value; fillProcs(); }); }
          fillProcs();
        }

        // ---- parameters table ----
        function fillParams(){
          const body = $("parambody");
          if (!body) return;
          const pf = $("paramfilter"), term = pf ? (pf.value||"").toLowerCase() : "";
          const rows = lastParams.filter(p => !term ||
            (p.key+" "+p.value).toLowerCase().includes(term));
          if (!rows.length){ body.innerHTML = '<tr><td colspan="2" class="empty">'+(lastParams.length?"No parameters match.":"No parameters recorded.")+'</td></tr>'; return; }
          body.innerHTML = rows.map(p =>
            '<tr><td class="proc-name">'+esc(p.key)+'</td><td class="pval">'+esc(p.value)+'</td></tr>').join("");
        }

        // ---- per-step table ----
        function fillProcs(){
          const body = $("procbody");
          if (!body) return;
          if (!lastProcs.length){ body.innerHTML = '<tr><td colspan="8" class="empty">No tasks recorded yet.</td></tr>'; return; }
          const procs = lastProcs.slice().sort(sortCmp(procSort));
          body.innerHTML = procs.map(stepRow).join("");
        }

        function sortCmp(key){
          if (key==="name")    return (a,b) => a.name.localeCompare(b.name);
          if (key==="time")    return (a,b) => (b.time_total_s||0)-(a.time_total_s||0);
          if (key==="mem")     return (a,b) => (b.mem_max_mb||0)-(a.mem_max_mb||0);
          if (key==="fail")    return (a,b) => (b.failed||0)-(a.failed||0) || (b.total||0)-(a.total||0);
          if (key==="running") return (a,b) => (b.running||0)-(a.running||0) || (b.pending||0)-(a.pending||0);
          if (key==="total")   return (a,b) => (b.total||0)-(a.total||0);
          return (a,b) => a.name.localeCompare(b.name);
        }

        function cnt(n, cls){ n = n||0; return '<td class="cnt '+(n?cls:"z")+'">'+n+'</td>'; }
        function dual(a, b){ // "avg · max" resource cell
          return '<td class="num stat"><b>'+esc(a)+'</b> <span class="mut">· '+esc(b)+'</span></td>';
        }
        function stepRow(p){
          const cpuA = p.cpu_avg!=null?p.cpu_avg+"%":"—", cpuM = p.cpu_max!=null?p.cpu_max+"%":"—";
          return '<tr><td class="proc-name">'+esc(p.name)+'</td>' +
            '<td>'+procBar(p, lastRunning)+'</td>' +
            cnt(p.running,"run") + cnt(p.pending,"pend") + cnt(p.failed,"fail") +
            dual(fmtDur(p.time_avg_s), fmtDur(p.time_max_s)) +
            dual(cpuA, cpuM) +
            dual(fmtMB(p.mem_avg_mb), fmtMB(p.mem_max_mb)) +
            '</tr>';
        }

        function completionCard(cp){
          const oc = {success:{t:"Completed successfully",k:"ok"}, error:{t:"Completed with errors",k:"fail"},
                      cancelled:{t:"Execution cancelled",k:"wait"}}[cp.outcome] || {t:"Finished",k:"idle"};
          return '<div class="card"><h3>Nextflow completion summary</h3>' +
            '<div style="margin-bottom:.8rem"><span class="pill '+oc.k+'">'+oc.t+'</span></div>' +
            '<div class="tiles">' +
              (cp.succeeded!=null?tile("ok",cp.succeeded,"Succeeded"):'') +
              (cp.failed!=null?tile("fail",cp.failed,"Failed"):'') +
              (cp.ignored!=null?tile("",cp.ignored,"Ignored"):'') +
              (cp.cpu_hours!=null?tile("",cp.cpu_hours,"CPU hours"):'') +
              (cp.nf_duration?tile("",cp.nf_duration,"Wall duration"):'') +
            '</div></div>';
        }

        function tile(k,n,l){ return '<div class="tile'+(k?" "+k:"")+'"><div class="n">'+esc(n)+'</div><div class="l">'+esc(l)+'</div></div>'; }
        function repBtn(url,label){ return '<a class="btn ghost" href="'+esc(url)+'" target="_blank" rel="noopener">'+esc(label)+' ↗</a>'; }
        function procBar(p, live){
          const t=p.total||0; if(!t) return '';
          const done=p.done||0, cached=p.cached||0, fail=p.failed||0;
          let run=p.running||0, pend=p.pending||0, stale=0;
          // Once a run has ended, tasks still marked running/pending in the trace
          // never finished (job killed, cancelled or timed out). Don't keep showing
          // the live blue "running" bar — fold them into a neutral "unfinished" segment.
          if (!live){ stale=run+pend; run=0; pend=0; }
          const w=(n)=>(100*n/t).toFixed(2)+"%";
          const sg=(cls,n,label)=> n>0 ? '<i class="'+cls+'" style="width:'+w(n)+'" title="'+n+' '+label+'"></i>' : '';
          const tip=done+' completed, '+run+' running, '+pend+' pending, '+fail+' failed'+
            (stale?', '+stale+' unfinished':'')+' of '+t;
          return '<div class="pbar" title="'+tip+'"><span class="bar">' +
            sg('ok',done,'completed')+sg('cached',cached,'cached')+sg('run',run,'running')+
            sg('pend',pend,'pending')+sg('fail',fail,'failed')+sg('stale',stale,'unfinished')+'</span>' +
            '<span class="frac">'+(done+cached)+'/'+t+'</span></div>';
        }
        function ganttHTML(tl, wallHint){
          if (!tl || !tl.length) return '';
          const span = Math.max(1, wallHint || Math.max.apply(null, tl.map(t => t.end_s||0)));
          const rows = tl.map(t => {
            const start = Math.max(0, t.start_s||0), end = Math.max(start, t.end_s||start);
            const left = (100*start/span), width = Math.max(0.6, 100*(end-start)/span);
            const dur = fmtDur(end-start), cls = (t.failed>0)?"fail":"ok";
            return '<div class="grow"><span class="glabel" title="'+esc(t.name)+'">'+esc(t.name)+'</span>' +
              '<span class="gtrack"><i class="gbar '+cls+'" style="left:'+left.toFixed(2)+'%;width:'+width.toFixed(2)+'%" ' +
              'title="'+esc(t.name)+' — starts '+fmtDur(start)+', runs '+dur+'"></i></span>' +
              '<span class="gdur">'+dur+'</span></div>';
          }).join("");
          const axis = '<div class="gaxis"><span>0</span><span>'+fmtDur(span/2)+'</span><span>'+fmtDur(span)+'</span></div>';
          return '<div class="gantt">'+rows+'</div>'+axis;
        }

        function workUrl(workdir,hash){
          if(!workdir||!hash||hash.indexOf("/")<0) return null;
          return "/pun/sys/dashboard/files/fs"+workdir+"/work/"+hash.split("/")[0]+"/";
        }

        // ---- navigation ----
        function openDetail(id){ current=id; backEl.hidden=false; listEl.hidden=true; detailEl.hidden=false;
          detailEl.innerHTML='<div class="skeleton"></div><div class="skeleton"></div>';
          window.scrollTo(0,0); refresh(); }
        function openList(){ current=null; backEl.hidden=true; detailEl.hidden=true; listEl.hidden=false; refresh(); }

        function refresh(){
          if (current){
            api("/api/runs/"+encodeURIComponent(current)).then(d => {
              if (d.error){ showStatus("error", d.error); return; }
              clearStatus(); renderDetail(d); stamp();
            }).catch(() => showStatus("error","Could not load run detail."));
          } else {
            api("/api/runs").then(d => {
              runsCache = d.runs||[];
              if (!runsCache.length){ summaryEl.innerHTML=""; runsEl.innerHTML="";
                showStatus("warning","No nf-core runs found. Launch an nf-core pipeline app, then reload."); return; }
              clearStatus(); renderSummary(runsCache); renderList(); stamp();
            }).catch(() => showStatus("error","Could not load your run list."));
          }
        }

        function schedule(){ if(timer){clearInterval(timer);timer=null;}
          const ms = parseInt(intervalSel.value,10); if(ms>0) timer=setInterval(refresh, ms); }

        backEl.addEventListener("click", openList);
        intervalSel.addEventListener("change", schedule);
        searchEl.addEventListener("input", renderList);

        const deep = new URLSearchParams(location.search).get("run");
        if (deep && /^[0-9a-fA-F-]{36}$/.test(deep)) {
          current = deep; backEl.hidden=false; listEl.hidden=true; detailEl.hidden=false;
          detailEl.innerHTML='<div class="skeleton"></div><div class="skeleton"></div>';
        }
        refresh(); schedule();
      })();
    JS
  end
end
