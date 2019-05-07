from unittest import TestCase
from tempfile import TemporaryDirectory

from luigi import Task
from luigi import build
from luigi.mock import MockTarget

from .utils import bytes_pls
from .utils import clean
from .utils import convert_numeric_code
from .utils import convert_numeric_code_with_pad
from .utils import get_salt_for_source
from .utils import TargetOutput


class UtilsTests(TestCase):
    def test_clean(self):

        dirty = ["1\n", "2\r", "\n\r3", "\xa04", "\xa05\r"]
        # if clean is not working, int will raise ValueError
        # if clean is chomping off wrong chars, sum will not be 15
        assert sum(map(lambda x: int(clean(x)), dirty)) == 15

    def test_bytes_pls(self):
        maybe = ["1", "2".encode()]
        thanks = map(lambda x: bytes_pls(x), maybe)
        for thank in thanks:
            assert isinstance(thank, bytes)

    def test_convert_numeric_code(self):
        numbers = [1, "002", "3", 4.0, "5.0"]
        converted = map(lambda x: convert_numeric_code(x), numbers)
        for convert in converted:
            assert isinstance(convert, str)

    def test_convert_numeric_code_with_pad(self):
        numbers = [1, "002", "3", 4.0, 5.8]
        converted = map(lambda x: convert_numeric_code_with_pad(x), numbers)
        for convert in converted:
            print(convert)
            assert isinstance(convert, str)
            assert len(convert) == 3

    def test_targetoutput(self):
        """
            Test demonstrating `TargetOutput` descriptor usage

        """
        with TemporaryDirectory() as tmp:

            class OtherTask(Task):

                output = TargetOutput(target_class=MockTarget)

                def run(self):
                    with self.output().open('w') as f:
                        f.write('other')

            class MyTask(Task):

                def requires(self):
                    return {"other": OtherTask()}

                output = TargetOutput(target_class=MockTarget)

                def run(self):
                    with self.output().open('w') as f:
                        f.write('my')

            graph = build([MyTask()], local_scheduler=True)
            assert graph is True
