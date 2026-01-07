# airflow/plugins/dbt_hook.py
import os
import subprocess
import json
from typing import Dict, List, Optional, Union, Any

from airflow.hooks.base import BaseHook


class DbtHook(BaseHook):
    """
    Interact with dbt CLI.
    
    :param profile: Profile from profiles.yml
    :param target: Target profile to use (usually 'dev' or 'prod')
    :param project_dir: Directory containing dbt_project.yml
    :param env_vars: Environment variables to pass to the dbt command
    """
    
    def __init__(
        self,
        profile: str,
        target: Optional[str] = None,
        project_dir: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__()
        self.profile = profile
        self.target = target
        self.project_dir = project_dir
        self.env_vars = env_vars or {}
        
    def _get_env(self) -> Dict[str, str]:
        """Get environment variables for dbt command."""
        env = os.environ.copy()
        env.update(self.env_vars)
        return env
        
    def _get_command_prefix(self) -> List[str]:
        """Get dbt command prefix with common options."""
        cmd = ["dbt"]
        
        if self.profile:
            cmd.extend(["--profile", self.profile])
            
        if self.target:
            cmd.extend(["--target", self.target])
            
        return cmd
        
    def run_command(
        self, 
        command: str, 
        models: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        select: Optional[List[str]] = None,
        vars: Optional[Dict[str, Any]] = None,
        full_refresh: Optional[bool] = None,
        fail_fast: Optional[bool] = None,
        **kwargs
    ) -> str:
        """
        Run a dbt command.
        
        :param command: dbt command to run (e.g., 'run', 'test', 'seed')
        :param models: List of models to include
        :param exclude: List of models to exclude
        :param select: Selection syntax for models
        :param vars: Variables to pass to dbt
        :param full_refresh: Whether to fully refresh incremental models
        :param fail_fast: Whether to fail fast on the first error
        :param kwargs: Additional arguments to pass to dbt
        :return: Command output
        """
        cmd = self._get_command_prefix()
        cmd.append(command)
    
        if self.project_dir:
            cmd.append(f"--project-dir={self.project_dir}")
            
        if models:
            cmd.extend(["--models", " ".join(models)])
            
        if exclude:
            cmd.extend(["--exclude", " ".join(exclude)])
            
        if select:
            cmd.extend(["--select", " ".join(select)])
            
        if vars:
            # Convert dict to JSON string for dbt --vars
            vars_str = json.dumps(vars)
            cmd.extend(["--vars", vars_str])
            
        # Add boolean flags
        if full_refresh:
            cmd.append("--full-refresh")
            
        if fail_fast:
            cmd.append("--fail-fast")
                
        self.log.info(f"Running dbt command: {' '.join(cmd)}")
        
        # Run the command
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self._get_env(),
            universal_newlines=True,
        )
        
        stdout, stderr = process.communicate()
        self.log.info(f"Running dbt command: {' '.join(cmd)} - {stdout}")
        
        if process.returncode != 0:
            self.log.error(f"dbt command failed: {stderr}")
            raise Exception(f"dbt command failed: {stderr}")
            
        return stdout
    
    def run(self, *args, **kwargs) -> str:
        """Run dbt run command."""
        return self.run_command("run", *args, **kwargs)
    
    def test(self, *args, **kwargs) -> str:
        """Run dbt test command."""
        return self.run_command("test", *args, **kwargs)
    
    def seed(self, *args, **kwargs) -> str:
        """Run dbt seed command."""
        return self.run_command("seed", *args, **kwargs)
    
    def snapshot(self, *args, **kwargs) -> str:
        """Run dbt snapshot command."""
        return self.run_command("snapshot", *args, **kwargs)
    
    def compile(self, *args, **kwargs) -> str:
        """Run dbt compile command."""
        return self.run_command("compile", *args, **kwargs)
    
    def docs_generate(self, *args, **kwargs) -> str:
        """Run dbt docs generate command."""
        return self.run_command("docs", "generate", *args, **kwargs)
    
    def parse_json_results(self, output: str) -> Dict:
        """Parse dbt JSON output into Python dict."""
        # Find and extract the JSON part from the output
        try:
            start_index = output.find('{')
            end_index = output.rfind('}') + 1
            if start_index >= 0 and end_index > start_index:
                json_str = output[start_index:end_index]
                return json.loads(json_str)
        except (ValueError, json.JSONDecodeError) as e:
            self.log.error(f"Failed to parse dbt output as JSON: {e}")
        
        return {}