import luigi
import wget
import os
import tarfile
import gzip
import pandas as pd
import io

class DownloadSupplementaryFiles(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_folder = 'supplementary_files'

    def output(self):
        return luigi.LocalTarget(f'{self.output_folder}/{self.dataset_name}_RAW.tar')

    def run(self):
        dataset_url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file'
        os.makedirs(self.output_folder, exist_ok=True)
        wget.download(dataset_url, out=self.output().path)

class UnpackTarArchive(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')  
    input_folder = 'supplementary_files'  

    def requires(self):
        return DownloadSupplementaryFiles(dataset_name=self.dataset_name)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.input_folder, f'{self.dataset_name}_unpacked'))

    def run(self):
        with tarfile.open(self.input().path, 'r') as tar:
            files = tar.getnames()

            print(f'В архиве {self.dataset_name} содержится {len(files)} файлов:')
            for file in files:
                print(file)

            for file in files:
                file_name = os.path.basename(file)
                folder_name = os.path.splitext(file_name)[0]  
                folder_path = os.path.join(self.input_folder, folder_name)

                os.makedirs(folder_path, exist_ok=True)

                with tar.extractfile(file) as f_in:
                    with gzip.open(f_in, 'rb') as f_gz:
                        content = f_gz.read().decode()

                        dfs = {}
                        write_key = None
                        fio = io.StringIO(content)
                        for l in fio.readlines():
                            if l.startswith('['):
                                if write_key:
                                    fio.seek(0)
                                    header = None if write_key == 'Heading' else 'infer'
                                    dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                                fio = io.StringIO()
                                write_key = l.strip('[]\n')
                                continue
                            if write_key:
                                fio.write(l)
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t')

                        for key, df in dfs.items():
                            if key == 'Probes':
                                table_name = f"{folder_name}_Probes.tsv"
                                table_path = os.path.join(folder_path, table_name)
                                df.to_csv(table_path, sep='\t', index=False)
                                
                                trimmed_df = df.drop(columns=['Definition', 'Ontology_Component', 'Ontology_Process',
                                                               'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id',
                                                               'Probe_Sequence'])
                                trimmed_table_name = f"{folder_name}_Probes_trimmed.tsv"
                                trimmed_table_path = os.path.join(folder_path, trimmed_table_name)
                                trimmed_df.to_csv(trimmed_table_path, sep='\t', index=False)
                            else:
                                table_name = f"{folder_name}_{key}.tsv"
                                table_path = os.path.join(folder_path, table_name)
                                df.to_csv(table_path, sep='\t', index=False)
                
                txt_file_path = os.path.join(folder_path, f"{folder_name}.txt")
                if os.path.exists(txt_file_path):
                    os.remove(txt_file_path)

if __name__ == '__main__':
    luigi.build([UnpackTarArchive()], local_scheduler=True)
