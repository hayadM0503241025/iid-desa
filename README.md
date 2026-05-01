# Streamlit Cloud Bundle

Folder ini adalah paket khusus untuk **Streamlit Community Cloud**.

## Isi utama

- `streamlit_app.py`
- `dashboard_streamlit.py`
- `id.py`
- `requirements.txt`
- `assets/`
- `pages/`
- `hasil_indeks_digital_uji2/`

## Entrypoint yang dipakai

Saat deploy di Streamlit Community Cloud, gunakan:

```text
streamlit_app.py
```

## Kenapa bundle ini aman untuk cloud

- Tidak bergantung pada `data_asli.csv`
- Default app akan membaca folder hasil siap pakai `hasil_indeks_digital_uji2`
- Tampilan tetap sama seperti versi Streamlit lokal
- Multipage tetap aktif karena file tambahan ada di folder `pages/`

## Langkah deploy

1. Upload isi folder ini ke repo GitHub khusus.
2. Buka Streamlit Community Cloud.
3. Klik `Create app`.
4. Pilih repo GitHub tersebut.
5. Set:

```text
Branch: main
Main file path: streamlit_app.py
```

6. Python version disarankan `3.12`.
7. Klik `Deploy`.

## Catatan

- Jika Anda mengubah hasil olah di project utama, salin ulang file di folder ini agar bundle cloud tetap sinkron.
- Folder hasil yang dibawa saat ini adalah `hasil_indeks_digital_uji2`.
