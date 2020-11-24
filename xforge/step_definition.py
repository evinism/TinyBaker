from abc import ABC, abstractmethod

class StepDefinition(ABC):
  input_file_set=set()
  output_file_set=set()
  
  def build(self, input={}, output={}, config=None):
    # Should declare self.input_files, self.output_files, and self.config
    pass
  
  @abstractmethod
  def script(self):
    pass
