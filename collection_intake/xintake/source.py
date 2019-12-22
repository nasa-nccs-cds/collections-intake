from intake.source import base


class CollectionDataSource(base.DataSource):
    container = 'ndarray'
    version = '0.0.1'
    partition_access = True
    name = 'collection_datasource'

    def __init__(self, organization, repo, source_kwargs={}, metadata=None):
        self.organization = organization
        self.repo = repo
        self.dskwargs = source_kwargs
        super(CollectionDataSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        self._dtypes = {'number': 'int',
                        'title': 'str',
                        'user': 'str',
                        'state': 'str',
                        'comments': 'int',
                        'created_at': 'datetime64[ns]',
                        'updated_at': 'datetime64[ns]',
                        'body': 'str'
                        }

        return base.Schema(
            datashape=None,
            dtype=self._dtypes,
            shape=(None, len(self._dtypes)),
            npartitions=1,  # This data is not partitioned, so there is only one partition
            extra_metadata={}
        )

    def _get_partition(self, _):
        from github import Github
        import pandas as pd
        gh = Github()
        repo = gh.get_repo('%s/%s' % (self.organization, self.repo))

        raw_data = repo.get_issues(**self.ghkwargs)
        data = {}
        for column in ['number', 'title', 'user', 'state', 'comments',
                       'created_at', 'updated_at', 'body']:
            # user is special case
            if column == 'user':
                data['user'] = [issue.user.login for issue in raw_data]
            else:
                data[column] = [getattr(issue, column) for issue in raw_data]

        return pd.DataFrame(data)

    def _close(self):
        pass