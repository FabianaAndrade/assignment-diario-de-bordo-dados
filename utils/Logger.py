import logging

class Logger:
    """
    Modulo para gerenciar a emissao de mensagens de log do pipeline.
    """
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)