# Enregistre la tâche planifiée Windows qui lance le pipeline ELT tous les
# jours à 08:00 (après l'heure d'export habituelle des fichiers ORDERS,
# observée entre 2h et 7h du matin).
#
# À exécuter une seule fois pour activer la planification :
#   powershell -ExecutionPolicy Bypass -File register_scheduled_task.ps1
#
# Pour supprimer la tâche plus tard :
#   Unregister-ScheduledTask -TaskName "PBB_Pipeline_Daily" -Confirm:$false

$ProjectDir = Split-Path -Parent $PSScriptRoot
$OrchestrationDir = Join-Path $ProjectDir "orchestration"
$PythonExe = Join-Path $ProjectDir ".venv\Scripts\python.exe"

$Action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "-X utf8 run_pipeline.py" `
    -WorkingDirectory $OrchestrationDir

$Trigger = New-ScheduledTaskTrigger -Daily -At 08:00

Register-ScheduledTask -TaskName "PBB_Pipeline_Daily" `
    -Action $Action `
    -Trigger $Trigger `
    -Description "Pipeline ELT Paris Basketball : extract -> load -> stage, quotidien" `
    -RunLevel Limited

Write-Host "Tache planifiee 'PBB_Pipeline_Daily' enregistree : tous les jours a 08:00."
