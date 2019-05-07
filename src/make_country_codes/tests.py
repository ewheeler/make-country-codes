import os
from unittest import TestCase
from tempfile import TemporaryDirectory

from luigi import format
from luigi import Parameter
from luigi import LocalTarget
from luigi import Task
from luigi import build
from luigi.mock import MockTarget

from .utils import bytes_pls
from .utils import clean
from .utils import convert_numeric_code
from .utils import convert_numeric_code_with_pad
from .utils import get_salt_for_source
from .utils import TargetOutput
from .utils import salted_SPLT
from .utils import SuffixPreservingLocalTarget
from .utils import BaseAtomicProviderLocalTarget


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


class TargetTests(TestCase):

    def test_suffix_preserving_atomic_file(self):
        """ ensure that suffix is preserved when using
            our suffix_preserving_atomic_file as atomic_provider """
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, 'asdf.txt')
            _, ext = os.path.splitext(fp)

            with SuffixPreservingLocalTarget(path=fp).temporary_path() as tp:
                _, ext2 = os.path.splitext(tp)
                assert ext == ext2

    def test_vanilla_atomic_file(self):
        """ ensure that suffix is NOT preserved when using
            luigi's atomic_file as atomic_provider
            (and ensure that it does work) """
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, 'asdf.txt')
            _, ext = os.path.splitext(fp)

            with BaseAtomicProviderLocalTarget(path=fp).temporary_path() as tp:
                assert os.path.exists(tp)
                _, ext2 = os.path.splitext(tp)
                assert ext != ext2

    def test_open_write(self):
        """ ensure that BaseAtomicProviderLocalTarget.open
            writes files as intended """
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, 'asdf.txt')
            _, ext = os.path.splitext(fp)
            with SuffixPreservingLocalTarget(path=fp).temporary_path() as tp:
                with open(tp, 'w') as o:
                    o.write('asdf')
                assert (os.path.isfile(tp) and os.path.getsize(tp) > 0)

    def test_open_read(self):
        """ ensure that BaseAtomicProviderLocalTarget.open
            reads files as intended """
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, 'asdf.txt')
            _, ext = os.path.splitext(fp)
            with SuffixPreservingLocalTarget(path=fp).temporary_path() as tp:
                content = 'asdf'
                found = None
                with open(tp, 'w') as o:
                    o.write(content)
                with open(tp, 'r') as i:
                    found = i.read()
                assert content == found

    def test_salted_SPLT(self):
        """ ensure that salted_SPLT salts targets as expected """
        class TestTaskOne(Task):
            __version__ = '1.0'

            def output(self):
                return MockTarget("output.txt")

        task = TestTaskOne()
        salty = salted_SPLT(task, file_pattern="output-{salt}.txt")

        root, ext = os.path.splitext(salty.path)
        # check that suffix is preserved
        assert ext == '.txt'
        path = root.split('-')
        # check that filename portion is correct
        assert path[0] == 'output'
        # check that 6 characters of salt are present
        salt = path[1]
        assert len(path[1]) == 6

        # check that salt changes when task version changes
        task.__version__ = '1.1'
        salty = salted_SPLT(task, file_pattern="output-{salt}.txt")
        root, ext = os.path.splitext(salty.path)
        path = root.split('-')
        assert salt != path[1]
