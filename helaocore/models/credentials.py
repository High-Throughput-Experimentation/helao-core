from pydantic import BaseSettings, SecretStr, parse_obj_as, PostgresDsn, validator
from typing import Optional, Dict, Union, Any
from textwrap import dedent
import urllib


class HelaoCredentials(BaseSettings):
    AWS_ACCESS_KEY_ID: SecretStr = parse_obj_as(SecretStr, "")
    AWS_SECRET_ACCESS_KEY: SecretStr = parse_obj_as(SecretStr, "")
    AWS_REGION: SecretStr = parse_obj_as(SecretStr, "")
    API_USER: str = "postgres"
    API_PASSWORD: SecretStr = parse_obj_as(SecretStr, "")
    API_HOST: str = "localhost"
    API_PORT: int = 5432
    API_DB: str = ""
    API_SCHEMA: str = "production"
    JUMPBOX_HOST: str = ""
    JUMPBOX_USER: str = ""
    JUMPBOX_KEYFILE: str = ""
    
    def set_api_port(self, port: int):
        self.API_PORT = port

    @property
    def api_dsn(self):
        pgdsn = PostgresDsn.build(
            scheme="postgresql",
            user=self.API_USER,
            password=urllib.parse.quote(self.API_PASSWORD.get_secret_value()) if self.API_PASSWORD else "",
            host="127.0.0.1",
            port=f"{self.API_PORT}",
            path=f"/{self.API_DB}",
        )
        pgdsn_schema = f"{pgdsn}?options=--search_path%3d{self.API_SCHEMA}"
        return pgdsn_schema

    def display(self, show_defaults: bool = False, show_passwords: bool = False, simple: bool = False):
        params = []
        for key, val in self.dict().items():
            if simple and key not in self._simple_params:
                continue
            if val is not None:
                str_val = (
                    f"{val.get_secret_value()}"
                    if show_passwords and ("PASSWORD" in key or "KEY" in key)
                    else val
                )
                if (show_defaults or key in self.__fields_set__) or key in self._always_set:
                    params.append(f"{key} = {str_val}")
                else:
                    params.append(f"# {key} = {str_val}")

        params_str = "\n".join(params)
        output = f"""# MPS Client Settings\n{params_str}"""
        return dedent(output)

    def __str__(self) -> str:
        return self.display()

    class Config:
        env_file = ".env"
