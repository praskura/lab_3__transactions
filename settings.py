from pydantic_settings import BaseSettings

class Settings(BaseSettings):
	database_dsn: str
	transaction_data_file_path: str
	class Config:
		env_file = '.env'
		env_file_encoding = 'utf-8'
