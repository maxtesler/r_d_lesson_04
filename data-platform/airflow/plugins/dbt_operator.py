# airflow/plugins/dbt_operator.py
from typing import Dict, List, Optional, Union, Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from dbt_hook import DbtHook


class DbtOperator(BaseOperator):
    """
    Execute dbt commands.
    
    :param command: dbt command to run (e.g., 'run', 'test', 'seed')
    :param profile: Profile from profiles.yml
    :param target: Target profile to use (usually 'dev' or 'prod')
    :param project_dir: Directory containing dbt_project.yml
    :param models: List of models to include
    :param exclude: List of models to exclude
    :param select: Selection syntax for models
    :param vars: Variables to pass to dbt
    :param env_vars: Environment variables to pass to the dbt command
    :param full_refresh: Whether to fully refresh incremental models
    :param fail_fast: Whether to fail fast on the first error
    """
    
    template_fields = ('command', 'models', 'exclude', 'select', 'vars', 'env_vars')
    
    @apply_defaults
    def __init__(
        self,
        command: str,
        profile: str,
        target: Optional[str] = None,
        project_dir: Optional[str] = None,
        models: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        select: Optional[List[str]] = None,
        vars: Optional[Dict[str, Any]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        full_refresh: Optional[bool] = None,
        fail_fast: Optional[bool] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.profile = profile
        self.target = target
        self.project_dir = project_dir
        self.models = models
        self.exclude = exclude
        self.select = select
        self.vars = vars
        self.env_vars = env_vars or {}
        self.full_refresh = full_refresh
        self.fail_fast = fail_fast
        
    def execute(self, context):
        hook = DbtHook(
            profile=self.profile,
            target=self.target,
            project_dir=self.project_dir,
            env_vars=self.env_vars,
        )
        
        # Parse additional kwargs to pass to the hook
        extra_kwargs = {}
        
        # Add boolean flags
        if self.full_refresh:
            extra_kwargs['full_refresh'] = self.full_refresh
            
        if self.fail_fast:
            extra_kwargs['fail_fast'] = self.fail_fast
            
        # Add any other custom parameters from self
        for key, value in self.__dict__.items():
            if (key not in ('command', 'profile', 'target', 'project_dir', 
                           'models', 'exclude', 'select', 'vars', 'env_vars', 
                           'full_refresh', 'fail_fast',
                           '_BaseOperator__init_kwargs', '_log') and 
                not key.startswith('_')):
                extra_kwargs[key] = value
        
        # If the command is a standard dbt command, call the specific method
        if hasattr(hook, self.command):
            result = getattr(hook, self.command)(
                models=self.models,
                exclude=self.exclude,
                select=self.select,
                vars=self.vars,
                **extra_kwargs
            )
        else:
            # Otherwise use the generic run_command method
            result = hook.run_command(
                self.command,
                models=self.models,
                exclude=self.exclude,
                select=self.select,
                vars=self.vars,
                **extra_kwargs
            )
        
        return result