# Job-Resource-Scheduler

Çalıştırmak için gerekli olan dosyalar://Örnek  olarak mevcuttur isteğe göre düzenlenebilir

Jobs.txt // space seperated txt file
JobID duration
Bu dosyanin her satırında ya bir job bilgisi
vardır, ya da “no job” string i bulunmaktadır.
Id ler sıralı olmak zorunda değildir

Dependencies.txt // space seperated txt file
JobID1 JobID2 //JobID2 bitmeden JobID1 başlayamaz.
Bu dosyada Job’lar arasındaki bağımlılıklar
verilmiştir. Her bir Job için 0,1 yada birden
fazla bağımlılık bulunabilir. Tüm bağımlı
olduğu Job’lar bitmeden o Job çalışmaya
başlayamaz.
