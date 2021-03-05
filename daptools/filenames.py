from pathlib import PurePath


class FilePath(object):
    def __init__(self, fname):
        self._path = PurePath(fname)

    @property
    def fname(self):
        return self._path.name

    @property
    def basedir(self):
        return self._path.parent

    @property
    def basename(self):
        return self._path.stem

    @property
    def ext(self):
        return self._path.suffix


class MedusaPath(FilePath):
    def __init__(self, fname):
        """Handle Medusa filenames

        Parameters
        ----------
        fname : str
            filename (e.g, uwl_200607_202746_53.sf)
        """
        super().__init__(fname)
        self._split_filepath()

    @property
    def receiver(self):
        return self._receiver

    @property
    def obs_id(self):
        return f"{self._mjd1}_{self._mjd2}"

    @property
    def file_index(self):
        return self._file_index

    @property
    def pointing(self):
        if self._b_index:
            return f"{self.receiver}_{self.obs_id}_{self._b_index}"
        return f"{self.receiver}_{self.obs_id}"

    def __str__(self):
        return f'{self._path}'

    def __repr__(self):
        return self.__str__()

    def _split_filepath(self):
        """ Split basename into components
        """
        parts_list = self.basename.split("_")
        if len(parts_list) == 5:
            receiver, mjd1, mjd2, b_index, file_index = parts_list
        elif len(parts_list) == 4:
            receiver, mjd1, mjd2, file_index = parts_list
            b_index = ""
        elif len(parts_list) == 3:
            receiver, mjd1, mjd2 = parts_list
            b_index = ""
            file_index = ""
        else:
            raise TypeError(f"File basename: {self.basename} contains "
                            f"{len(parts_list)}' segments. Not supported!")
        self._receiver = receiver
        self._mjd1 = mjd1
        self._mjd2 = mjd2
        self._b_index = b_index
        self._file_index = file_index


class HIPSRPath(FilePath):
    def __init__(self, fname):
        """Handle HIPSR_SRCH filenames

        Parameters
        ----------
        fname : str
            filename (e.g, bpsr140423_140118_beam09.sf)
        """
        super().__init__(fname)
        self._split_filepath()

    @property
    def receiver(self):
        return self._receiver

    @property
    def obs_id(self):
        return f"{self._mjd1}_{self._mjd2}"

    @property
    def beam_index(self):
        return self._beam_index

    @property
    def pointing(self):
        return f"{self.receiver}{self.obs_id}"

    def __str__(self):
        return f'{self._path}'

    def __repr__(self):
        return self.__str__()

    def _split_filepath(self):
        """ Split basename into components
        """
        parts_list = self.basename.split("_")
        if len(parts_list) == 3:
            receivermjd1, mjd2, beam = parts_list
            receiver = "bpsr"
            mjd1 = receivermjd1.split("bpsr")[-1]
            beam_index = beam.split("beam")[-1]
        else:
            raise TypeError(f"File basename: {self.basename} contains "
                            f"{len(parts_list)}' segments. Not supported!")
        self._receiver = receiver
        self._mjd1 = mjd1
        self._mjd2 = mjd2
        self._beam = beam
        self._beam_index = beam_index
