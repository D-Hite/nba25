{
  echo '$ tree -I '\''venv|*.pyc|*.log'\'''
  tree -I 'venv|*.pyc|*.log'
} > treed.txt