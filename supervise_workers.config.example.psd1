@{
    # RepoRoot can usually stay empty when the launcher is used from this repo.
    RepoRoot = ''

    # Executables
    PythonExe = 'python'
    SshExe = 'ssh'

    # SSH tunnel settings
    SkipTunnel = $false
    SshUser = 'ucloud'
    SshHost = 'ssh.cloud.sdu.dk'
    SshPort = 1234
    KeyPath = 'C:\Users\usr\.ssh\id_key'
    LocalDbPort = 15432
    RemoteDbHost = 'localhost'
    RemoteDbPort = 5432
    ExtraSshArgs = @()

    # Optional explicit DATABASE_URL override. Leave empty to reuse .env and
    # automatically rewrite the localhost port to LocalDbPort.
    DatabaseUrl = ''

    # Supervisor behavior
    RestartDelaySeconds = 10
    PollIntervalSeconds = 2
    SupervisorLogDir = ''

    # Downloader
    StartDownloader = $true
    RunDir = 'D:\yt_download_worker_a'
    DownloadArgs = @('--workers', '8')

    # Backup and reap is optional. Leave StartBackupAndReap = $false to skip it.
    StartBackupAndReap = $false
    BackupDir = 'O:\ARTS_SoMe-Influence\YT_Download_all_videos\DB_backup'
    BackupArgs = @('--interval-minutes', '60', '--reap')

    # Optional archiver
    StartArchiver = $false
    ArchiveDir = 'O:\ARTS_SoMe-Influence\YT_Download_all_videos\archive'
    ArchiverArgs = @()
}
