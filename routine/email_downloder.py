import imapclient
import pyzmail

m = imapclient.IMAPClient("imap.gmail.com",ssl=True)
m.login("jinyoungkim0308@gmail.com","@wlsud890312")
mailbox = m.select_folder("INBOX")
UIDs = m.search(["FROM","tmdal980@gmail.com"])

for uid in UIDs :
    raw_msg = m.fetch(uid,['BODY[]'])
    msg = pyzmail.PyzMessage.factory(raw_msg[uid][b'BODY[]'])
    print(msg.get_addresses('to'))
    print(msg.get_addresses('from'))
    print(msg.get_subject())
    print(msg.text_part.get_payload().decode(str(msg.text_part.charset)))
    print()
    for mp in msg.mailparts :
        if mp.filename != None and mp.filename.find('xlsx') != -1 :
            print(mp.filename, len(mp.get_payload()))
            file = open("/home/projects/pcg_transform/Operation_data/operation_list/"+mp.filename,"wb")
            file.write(mp.get_payload())
            file.close()
            m.delete_messages(uid)
            m.expunge()


