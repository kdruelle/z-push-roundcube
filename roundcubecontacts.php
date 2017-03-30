<?php
/**
 * This backend is for roundcube contacts in MySQL
 *
 * Based on Zarafa's vcard directory backend
 * Roundcube modifications by Alex Charrett.
 * TODO: Switch to PDO, clean up SQL injection, import IMAP message code.
 * Buggy mobile number detection code is around line 560.
 *
 * @author     Alex Charrett <alex@transposed.org>
 * @author     Kitson Consulting <kitson-consulting.co.uk>
 * @copyright  2007-2012 Zarafa Deutschland GmbH
 * @copyright  2013-2015 Alex Charrett
 * @copyright  2015 Kitson Consulting Limited
 * @date       2015-09-20
 * @file       roundcubecontacts.php
 * @licence    https://www.gnu.org/licenses/agpl-3.0.en.html Gnu Affero Public Licence v3
 * @project    Z-Push
 */

// config file
require_once("backend/roundcubecontacts/config.php");

class BackendRoundcubeContacts extends BackendDiff
{
    private $logged_in_user;
    private $db_user_id;
    private $contactsdb;
    private $authdb;
    private $changessinkinit;
    private $sinkstates = null;
    private $sinkdata;

    /**----------------------------------
     * default backend methods
     */

    public function BackendRoundcubeContacts()
    {
        $this->changessinkinit = false;
        $this->sinkdata = array();

        parent::__construct();
    }

    /**
     * Authenticates the user
     *
     * @param string        $username
     * @param string        $domain
     * @param string        $password
     *
     * @access public
     * @return boolean
     */
    public function Logon($username, $domain, $password)
    {
        ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->Login('%s', '%s', '***')", $username, $domain));
        $this->dbConnectContacts();
        $login_success = false;

        if ($this->contactsdb !== false && strcmp(ROUNDCUBE_CONTACT_USER_AUTH, "database") == 0) {
            $this->dbConnectAuth();

            $password_from_db = $this->getEncryptedPassword($username);
            $crypted = md5($password);

            if (strcmp($crypted, $password_from_db) == 0) {
                $login_success = true;
            }
        } elseif (strcmp(ROUNDCUBE_CONTACT_USER_AUTH, "imap") == 0) {
            $imap_handle = imap_open(ROUNDCUBE_CONTACT_IMAP_SERVER, $username, $password);

            if (!($imap_handle === false)) {
                imap_close($imap_handle);
                $login_success = true;
            }
        }

        if ($login_success) {
            ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->Logon(): User '%s' is authenticated", $username));
            $this->logged_in_user = strtolower($username);

            $this->db_user_id = $this->getUserId($this->logged_in_user);
            ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->Logon(): User '$username' has ID $this->db_user_id in contacts DB");

            return true;
        } else {
            ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->Logon(): User '%s' failed to authenticate", $username));
            return false;
        }
    }

    /**
     * Logs off
     *
     * @access public
     * @return boolean
     */
    public function Logoff()
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->Logoff()");
        return true;
    }

    /**
     * Sends an e-mail
     * Not implemented here
     *
     * @param SyncSendMail  $sm     SyncSendMail object
     *
     * @access public
     * @return boolean
     * @throws StatusException
     */
    public function SendMail($sm)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->SendMail()");
        return false;
    }

    /**
     * Returns the content of the named attachment as stream
     * not implemented
     *
     * @param string        $attname
     *
     * @access public
     * @return SyncItemOperationsAttachment
     * @throws StatusException
     */
    public function GetAttachmentData($attname)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->GetAttachmentData()");
        return false;
    }

    /**
     * Returns the waste basket
     *
     * @access public
     * @return string
     */
    public function GetWasteBasket()
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->GetWasteBasket()");
        return 'DeletedItems';
    }

    /**
     * Indicates if the backend has a ChangesSink.
     * A sink is an active notification mechanism which does not need polling.
     * The KolabCalendar backend simulates a sink by polling changed dates from the events
     *
     * @access public
     * @return boolean
     */
    public function HasChangesSink()
    {
        return true;
    }


    /**
     * The folder should be considered by the sink.
     * Folders which were not initialized should not result in a notification
     * of IBacken->ChangesSink().
     *
     * @param string        $folderid
     *
     * @access public
     * @return boolean      false if found can not be found
     */
    public function ChangesSinkInitialize($folderid) {
        ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->ChangesSinkInitialize(): folderid '%s'", $folderid));

        if($folderid == 'Contacts'){
            $this->changessinkinit = true;
        }
        return $this->changessinkinit;
    }

    /**
     * The actual ChangesSink.
     * For max. the $timeout value this method should block and if no changes
     * are available return an empty array.
     * If changes are available a list of folderids is expected.
     *
     * @param int           $timeout        max. amount of seconds to block
     *
     * @access public
     * @return array
     */
    public function ChangesSink($timeout = 30) {
        $notifications = array();
        $stopat = time() + $timeout - 1;

        //We can get here and the ChangesSink not be initialized yet
        if (!$this->changessinkinit) {
            ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->ChangesSink - Not initialized ChangesSink, sleep and exit"));
            // We sleep and do nothing else
            sleep($timeout);
            return $notifications;
        }

        $query = sprintf("select changed from %scontacts where del = '0' order by changed DESC limit 1", ROUNDCUBE_CONTACT_BD_PREFIX);

        $result = $this->contactsdb->query($query);
        $result_row = $result->fetch();

        if ($this->sinkstates == null) {
            $this->sinkstates = $result_row[0];
        }

        if ($this->sinkstates != $result_row[0]){
            $notifications[] = 'Contacts';
            $this->sinkstates = $result_row[0];
            ZLog::Write(LOGLEVEL_INFO, "BackendRoundcubeContacts->ChangesSink(: ChangesSink detected!!)");
        }

        if (empty($notifications)) {
            while ($stopat > time()) {
                sleep(1);
            }
        }
        return $notifications;

    }

    /**----------------------------------
     * implemented DiffBackend methods
     */

    /**
     * Returns a list (array) of folders.
     * In simple implementations like this one, probably just one folder is returned.
     *
     * @access public
     * @return array
     */
    public function GetFolderList()
    {
        ZLog::Write(LOGLEVEL_DEBUG, 'BackendRoundcubeContacts->GetFolderList()');
        $contacts = array();
        $folder = $this->StatFolder('Contacts');
        if (count($folder)) {
            $contacts[] = $folder;
        }

        return $contacts;
    }

    /**
     * Returns an actual SyncFolder object
     *
     * @param string        $id           id of the folder
     *
     * @access public
     * @return object       SyncFolder with information
     */
    public function GetFolder($id)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->GetFolder($id)");
        $folder = false;
        if ($id === "Contacts") {
            $folder = new SyncFolder();
            $folder->serverid = $id;
            $folder->parentid = "0";
            $folder->displayname = $id;
            $folder->type = SYNC_FOLDER_TYPE_CONTACT;
        }

        return $folder;
    }

    /**
     * Returns folder stats. An associative array with properties is expected.
     *
     * @param string        $id             id of the folder
     *
     * @access public
     * @return array
     */
    public function StatFolder($id)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->StatFolder($id)");
        $folder = $this->GetFolder($id);

        $stat = array();
        if ($folder) {
            $stat["id"] = $id;
            $stat["parent"] = $folder->parentid;
            $stat["mod"] = $folder->displayname;
        }

        return $stat;
    }

    /**
     * Creates or modifies a folder
     * not implemented
     *
     * @param string        $folderid       id of the parent folder
     * @param string        $oldid          if empty -> new folder created, else folder is to be renamed
     * @param string        $displayname    new folder name (to be created, or to be renamed to)
     * @param int           $type           folder type
     *
     * @access public
     * @return boolean                      status
     * @throws StatusException              could throw specific SYNC_FSSTATUS_* exceptions
     *
     */
    public function ChangeFolder($folderid, $oldid, $displayname, $type)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->ChangeFolder($folderid, $oldid, $displayname, $type)");
        return false;
    }

    /**
     * Deletes a folder
     *
     * @param string        $id
     * @param string        $parent         is normally false
     *
     * @access public
     * @return boolean                      status - false if e.g. does not exist
     * @throws StatusException              could throw specific SYNC_FSSTATUS_* exceptions
     *
     */
    public function DeleteFolder($id, $parentid)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->DeleteFolder($id, $parentid)");
        return false;
    }

    /**
     * Returns a list (array) of messages
     *
     * @param string        $folderid       id of the parent folder
     * @param long          $cutoffdate     timestamp in the past from which on messages should be returned
     *
     * @access public
     * @return array/false  array with messages or false if folder is not available
     */
    public function GetMessageList($folderid, $cutoffdate)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->GetMessageList($folderid)");
        $messages = array();

        // if the folder is "DeletedItems" then return items flagged as deleted
        if (strcmp("$folderid", 'DeletedItems') === 0) {
            $del = 1;
        } else {
            $del = 0;
        }

        $query = sprintf("SELECT contact_id, changed from %scontacts where user_id = :user_id and del = :del", ROUNDCUBE_CONTACT_BD_PREFIX);
        $stmt = $this->contactsdb->prepare($query);
        $stmt->execute(array('user_id' => $this->db_user_id, 'del' => $del));

        while ($result_rows = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $message = array();
            $message['id'] = $result_rows['contact_id'];
            $message['mod'] = strtotime($result_rows['changed']);
            $message['flags'] = 1; // always 'read'

            $messages[] = $message;
        }

        return $messages;
    }

    /**
     * Returns the actual SyncXXX object type.
     *
     * @param string            $folderid           id of the parent folder
     * @param string            $id                 id of the message
     * @param ContentParameters $contentparameters  parameters of the requested message (truncation, mimesupport etc)
     *
     * @access public
     * @return object/false     false if the message could not be retrieved
     */
    public function GetMessage($folderid, $id, $contentparameters)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->GetMessage($folderid, $id, ..)");
        if ($folderid !== "Contacts") {
            return;
        }

        $types = array (
            'dom' => 'type',
            'intl' => 'type',
            'postal' => 'type',
            'parcel' => 'type',
            'home' => 'type',
            'work' => 'type',
            'pref' => 'type',
            'voice' => 'type',
            'fax' => 'type',
            'msg' => 'type',
            'cell' => 'type',
            'pager' => 'type',
            'bbs' => 'type',
            'modem' => 'type',
            'car' => 'type',
            'isdn' => 'type',
            'video' => 'type',
            'aol' => 'type',
            'applelink' => 'type',
            'attmail' => 'type',
            'cis' => 'type',
            'eworld' => 'type',
            'internet' => 'type',
            'ibmmail' => 'type',
            'mcimail' => 'type',
            'powershare' => 'type',
            'prodigy' => 'type',
            'tlx' => 'type',
            'x400' => 'type',
            'gif' => 'type',
            'cgm' => 'type',
            'wmf' => 'type',
            'bmp' => 'type',
            'met' => 'type',
            'pmb' => 'type',
            'dib' => 'type',
            'pict' => 'type',
            'tiff' => 'type',
            'pdf' => 'type',
            'ps' => 'type',
            'jpeg' => 'type',
            'qtime' => 'type',
            'mpeg' => 'type',
            'mpeg2' => 'type',
            'avi' => 'type',
            'wave' => 'type',
            'aiff' => 'type',
            'pcm' => 'type',
            'x509' => 'type',
            'pgp' => 'type',
            'text' => 'value',
            'inline' => 'value',
            'url' => 'value',
            'cid' => 'value',
            'content-id' => 'value',
            '7bit' => 'encoding',
            '8bit' => 'encoding',
            'quoted-printable' => 'encoding',
            'base64' => 'encoding',
        );

        // Parse the vcard
        $message = new SyncContact();

        $query = sprintf("SELECT vcard, name, email, firstname, surname from %scontacts where user_id = :user_id and contact_id = :id and del = '0'", ROUNDCUBE_CONTACT_BD_PREFIX);
        $stmt = $this->contactsdb->prepare($query);
        $stmt->execute(array('user_id' => $this->db_user_id, 'id' => $id));

        $results = 0;
        while ($result_rows = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $data = $result_rows['vcard'];
            $name = $result_rows['name'];
            $email = $result_rows['email'];
            $firstname = $result_rows['firstname'];
            $surname = $result_rows['surname'];
            ++$results;
        }

        if (($results >= 1) && ((strlen($data)) === 0)) {
            $data = "BEGIN:VCARD\nVERSION:3.0\nN:$surname;$firstname;;;\nFN:$name\nEMAIL;TYPE=INTERNET:$email\nEND:VCARD";
        }

        $data = str_replace("\x00", '', $data);
        $data = str_replace("\r\n", "\n", $data);
        $data = str_replace("\r", "\n", $data);
        $data = preg_replace('/(\n)([ \t])/i', '', $data);

        $lines = explode("\n", $data);

        $vcard = array();
        foreach ($lines as $line) {
            if (trim($line) === '') {
                continue;
            }

            $pos = strpos($line, ':');
            if ($pos === false) {
                continue;
            }

            $field = trim(substr($line, 0, $pos));
            $value = trim(substr($line, $pos + 1));

            $fieldparts = preg_split('/(?<!\\\\)(\;)/i', $field, -1, PREG_SPLIT_NO_EMPTY);

            $type = strtolower(array_shift($fieldparts));

            $fieldvalue = array();

            foreach ($fieldparts as $fieldpart) {
                if (preg_match('/([^=]+)=(.+)/', $fieldpart, $matches)) {
                    if (!in_array(strtolower($matches[1]), array('value', 'type', 'encoding', 'language'))) {
                        continue;
                    }

                    if (isset($fieldvalue[strtolower($matches[1])]) && is_array($fieldvalue[strtolower($matches[1])])) {
                        $fieldvalue[strtolower($matches[1])] = array_merge($fieldvalue[strtolower($matches[1])], preg_split('/(?<!\\\\)(\,)/i', $matches[2], -1, PREG_SPLIT_NO_EMPTY));
                    } else {
                        $fieldvalue[strtolower($matches[1])] = preg_split('/(?<!\\\\)(\,)/i', $matches[2], -1, PREG_SPLIT_NO_EMPTY);
                    }
                } else {
                    if (!isset($types[strtolower($fieldpart)])) {
                        continue;
                    }

                    $fieldvalue[$types[strtolower($fieldpart)]][] = $fieldpart;
                }
            }

            switch ($type) {
                case 'categories':
                    //case 'nickname':
                    $val = preg_split('/(?<!\\\\)(\,)/i', $value);
                    $val = array_map('w2ui', $val);
                    break;
                default:
                    $val = preg_split('/(?<!\\\\)(\;)/i', $value);
                    break;
            }

            if (isset($fieldvalue['encoding'][0])) {
                switch (strtolower($fieldvalue['encoding'][0])) {
                    case 'q':
                    case 'quoted-printable':
                        foreach ($val as $i => $v) {
                            $val[$i] = quoted_printable_decode($v);
                        }
                        break;
                    case 'b':
                    case 'base64':
                        foreach ($val as $i => $v) {
                            $val[$i] = base64_decode($v);
                        }
                        break;
                }
            } else {
                foreach ($val as $i => $v) {
                    $val[$i] = $this->unescape($v);
                }
            }

            $fieldvalue['val'] = $val;
            $vcard[$type][] = $fieldvalue;
        }

        if (isset($vcard['email'][0]['val'][0])) {
            $message->email1address = $vcard['email'][0]['val'][0];
        }

        if (isset($vcard['email'][1]['val'][0])) {
            $message->email2address = $vcard['email'][1]['val'][0];
        }

        if (isset($vcard['email'][2]['val'][0])) {
            $message->email3address = $vcard['email'][2]['val'][0];
        }

        // Calling array_map repeatedly throughout this clause is inefficient, but it works.
        // This code shouldn't get called all that often. Refactoring welcome.
        if (isset($vcard['tel'])) {
            foreach ($vcard['tel'] as $tel) {
                if (!isset($tel['type'])) {
                    $tel['type'] = array();
                }

                if (in_array('car', array_map('mb_strtolower', $tel['type']))) {
                    $message->carphonenumber = $tel['val'][0];
                } elseif (in_array('pager', array_map('mb_strtolower', $tel['type']))) {
                    $message->pagernumber = $tel['val'][0];
                } elseif (in_array('cell', array_map('mb_strtolower', $tel['type']))) {
                    $message->mobilephonenumber = $tel['val'][0];
                } elseif (in_array('home', array_map('mb_strtolower', $tel['type']))) {
                    if (in_array('fax', array_map('mb_strtolower', $tel['type']))) {
                        $message->homefaxnumber = $tel['val'][0];
                    } elseif (empty($message->homephonenumber)) {
                        $message->homephonenumber = $tel['val'][0];
                    } else {
                        $message->home2phonenumber = $tel['val'][0];
                    }
                } elseif (in_array('work', array_map('mb_strtolower', $tel['type']))) {
                    if (in_array('fax', array_map('mb_strtolower', $tel['type']))) {
                        $message->businessfaxnumber = $tel['val'][0];
                    } elseif (empty($message->businessphonenumber)) {
                        $message->businessphonenumber = $tel['val'][0];
                    } else {
                        $message->business2phonenumber = $tel['val'][0];
                    }
                } elseif (empty($message->homephonenumber)) {
                    $message->homephonenumber = $tel['val'][0];
                } elseif (empty($message->home2phonenumber)) {
                    $message->home2phonenumber = $tel['val'][0];
                } else {
                    $message->radiophonenumber = $tel['val'][0];
                }
            }
        }

        //;;street;city;state;postalcode;country
        if (isset($vcard['adr'])) {
            foreach ($vcard['adr'] as $adr) {
                if (empty($adr['type'])) {
                    $a = 'other';
                } elseif (in_array('home', $adr['type'])) {
                    $a = 'home';
                } elseif (in_array('work', $adr['type'])) {
                    $a = 'business';
                } else {
                    $a = 'other';
                }

                if (!empty($adr['val'][2])) {
                    $b = $a . 'street';
                    $message->$b = w2ui($adr['val'][2]);
                }

                if (!empty($adr['val'][3])) {
                    $b = $a . 'city';
                    $message->$b = w2ui($adr['val'][3]);
                }

                if (!empty($adr['val'][4])) {
                    $b = $a . 'state';
                    $message->$b = w2ui($adr['val'][4]);
                }

                if (!empty($adr['val'][5])) {
                    $b = $a . 'postalcode';
                    $message->$b = w2ui($adr['val'][5]);
                }

                if (!empty($adr['val'][6])) {
                    $b = $a . 'country';
                    $message->$b = w2ui($adr['val'][6]);
                }
            }
        }

        if (!empty($vcard['fn'][0]['val'][0])) {
            $message->fileas = w2ui($vcard['fn'][0]['val'][0]);
        }

        if (!empty($vcard['n'][0]['val'][0])) {
            $message->lastname = w2ui($vcard['n'][0]['val'][0]);
        }

        if (!empty($vcard['n'][0]['val'][1])) {
            $message->firstname = w2ui($vcard['n'][0]['val'][1]);
        }

        if (!empty($vcard['n'][0]['val'][2])) {
            $message->middlename = w2ui($vcard['n'][0]['val'][2]);
        }

        if (!empty($vcard['n'][0]['val'][3])) {
            $message->title = w2ui($vcard['n'][0]['val'][3]);
        }

        if (!empty($vcard['n'][0]['val'][4])) {
            $message->suffix = w2ui($vcard['n'][0]['val'][4]);
        }

        if (!empty($vcard['bday'][0]['val'][0])) {
            $tz = date_default_timezone_get();
            date_default_timezone_set('UTC');
            $message->birthday = strtotime($vcard['bday'][0]['val'][0]);
            date_default_timezone_set($tz);
        }

        if (!empty($vcard['org'][0]['val'][0])) {
            $message->companyname = w2ui($vcard['org'][0]['val'][0]);
        }

        if (!empty($vcard['note'][0]['val'][0])) {
            $message->body = w2ui($vcard['note'][0]['val'][0]);
            $message->bodysize = strlen($vcard['note'][0]['val'][0]);
            $message->bodytruncated = 0;
        }

        if (!empty($vcard['role'][0]['val'][0])) {
            $message->jobtitle = w2ui($vcard['role'][0]['val'][0]);//$vcard['title'][0]['val'][0]
        }

        if (!empty($vcard['url'][0]['val'][0])) {
            $message->webpage = w2ui($vcard['url'][0]['val'][0]);
        }

        if (!empty($vcard['categories'][0]['val'])) {
            $message->categories = $vcard['categories'][0]['val'];
        }

        if (!empty($vcard['photo'][0]['val'][0])) {
            $message->picture = base64_encode($vcard['photo'][0]['val'][0]);
        }

        return $message;
    }

    /**
     * Returns message stats, analogous to the folder stats from StatFolder().
     *
     * @param string        $folderid       id of the folder
     * @param string        $id             id of the message
     *
     * @access public
     * @return array
     */
    public function StatMessage($folderid, $id)
    {
        ZLog::Write(LOGLEVEL_DEBUG, 'BackendRoundcubeContacts->StatMessage('.$folderid.', '.$id.')');
        if ($folderid != "Contacts") {
            return false;
        }

        $query = sprintf("SELECT contact_id, changed from %scontacts where user_id= :user_id AND del='0' AND contact_id= :id", ROUNDCUBE_CONTACT_BD_PREFIX);
        $stmt = $this->contactsdb->prepare($query);
        $stmt->execute(array('user_id' => $this->db_user_id, 'id' => $id));

        $message = array();

        while ($result_rows = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $message["id"] = $result_rows['contact_id'];
            $message["mod"] = strtotime($result_rows['changed']);
            $message["flags"] = 1; // always 'read'
        }

        return $message;
    }

    /**
     * Called when a message has been changed on the mobile.
     * This functionality is not available for emails.
     *
     * @param string        $folderid       id of the folder
     * @param string        $id             id of the message
     * @param SyncXXX       $message        the SyncObject containing a message
     *
     * @access public
     * @return array                        same return value as StatMessage()
     * @throws StatusException              could throw specific SYNC_STATUS_* exceptions
     */
    public function ChangeMessage($folderid, $id, $message, $contentParameters)
    {
        ZLog::Write(LOGLEVEL_INFO, 'BackendRoundcubeContacts->ChangeMessage('.$folderid.', '.$id.', ..)');
        $mapping = array(
            'fileas' => 'FN',
            'lastname;firstname;middlename;title;suffix' => 'N',
            'email1address' => 'EMAIL;INTERNET',
            'email2address' => 'EMAIL;INTERNET',
            'email3address' => 'EMAIL;INTERNET',
            'businessphonenumber' => 'TEL;WORK',
            'business2phonenumber' => 'TEL;WORK',
            'businessfaxnumber' => 'TEL;WORK;FAX',
            'homephonenumber' => 'TEL;HOME',
            'home2phonenumber' => 'TEL;HOME',
            'homefaxnumber' => 'TEL;HOME;FAX',
            'mobilephonenumber' => 'TEL;CELL',
            'carphonenumber' => 'TEL;CAR',
            'pagernumber' => 'TEL;PAGER',
            ';;businessstreet;businesscity;businessstate;businesspostalcode;businesscountry' => 'ADR;WORK',
            ';;homestreet;homecity;homestate;homepostalcode;homecountry' => 'ADR;HOME',
            ';;otherstreet;othercity;otherstate;otherpostalcode;othercountry' => 'ADR',
            'companyname' => 'ORG',
            'body' => 'NOTE',
            'jobtitle' => 'ROLE',
            'webpage' => 'URL',
        );

        $data = "BEGIN:VCARD\nVERSION:2.1\nPRODID:Z-Push\n";
        foreach ($mapping as $k => $v) {
            $val = '';
            $ks = explode(';', $k);
            foreach ($ks as $i) {
                if (!empty($message->$i)) {
                    $val .= $this->escape($message->$i);
                }

                $val .= ';';
            }

            if (empty($val)) {
                continue;
            }

            $val = substr($val, 0, -1);
            if (strlen($val) > 50) {
                $data .= $v . ":\n\t" . substr(chunk_split($val, 50, "\n\t"), 0, -1);
            } else {
                $data .= $v . ':' . $val . "\n";
            }
        }

        if (!empty($message->categories)) {
            $data .= 'CATEGORIES:' . implode(',', $this->escape($message->categories)) . "\n";
        }

        if (!empty($message->picture)) {
            $data .= 'PHOTO;ENCODING=BASE64;TYPE=JPEG:' . "\n\t" . substr(chunk_split($message->picture, 50, "\n\t"), 0, -1);
        }

        if (isset($message->birthday)) {
            $data .= 'BDAY:' . date('Y-m-d', $message->birthday) . "\n";
        }

        $data .= "END:VCARD";

        // not supported: anniversary, assistantname, assistnamephonenumber, children, department, officelocation, radiophonenumber, spouse, rtf

        $fullname = $message->fileas;
        $surname = $message->lastname;
        $firstname = $message->firstname;

        // Roundcube appears to file multiple email addresses as CSV in the email field
        $emailaddress = $message->email1address;
        if ((strlen($message->email2address)) > 0) {
            $emailaddress .= ",$message->email2address";
        }

        if ((strlen($message->email3address)) > 0) {
            $emailaddress .= ",$message->email3address";
        }

        // If there's no $id, then this is a new record.
        if (!$id) {
            $query1 = sprintf("INSERT INTO %scontacts (vcard,name,email,firstname,surname,user_id,del,changed) VALUES ('$data','$fullname','$emailaddress','$firstname','$surname','$this->db_user_id','0',NOW())", ROUNDCUBE_CONTACT_BD_PREFIX);
        } else {
            // Otherwise, we're updating an existing id.
            $query1 = sprintf("UPDATE %scontacts SET vcard='$data',name='$fullname',email='$emailaddress',firstname='$firstname',surname='$surname',changed=NOW() WHERE user_id='$this->db_user_id' and contact_id='$id'", ROUNDCUBE_CONTACT_BD_PREFIX);
        }

        ZLog::Write(LOGLEVEL_DEBUG, "ChangeMessage $query1");
        $this->contactsdb->query($query1);
        //mysql_query($query1, $this->contactsdb) or ZLog::Write(LOGLEVEL_DEBUG, (mysql_error($this->contactsdb)));

        // If this is a new entry, we want to know the ID of the record we just added
        if (!$id) {
            $query2 = sprintf("SELECT contact_id FROM %scontacts WHERE user_id='$this->db_user_id' AND name='$fullname' AND email='$emailaddress' AND firstname='$firstname' AND surname='$surname' AND vcard='$data' ORDER BY contact_id DESC LIMIT 1", ROUNDCUBE_CONTACT_BD_PREFIX);

            $result2 = $this->contactsdb->query($query2);
            #$result2 = mysql_query($query2, $this->contactsdb) or print(mysql_error($this->contactsdb));

            while ($result_rows = $result2->fetch()) {
                $id = $result_rows[0];
                ZLog::Write(LOGLEVEL_DEBUG, "ChangeMessage new contact id is $id");
            }
        }

        return $this->StatMessage($folderid, $id);
    }

    /**
     * Changes the 'read' flag of a message on disk
     *
     * @param string        $folderid       id of the folder
     * @param string        $id             id of the message
     * @param int           $flags          read flag of the message
     *
     * @access public
     * @return boolean                      status of the operation
     * @throws StatusException              could throw specific SYNC_STATUS_* exceptions
     */
    public function SetReadFlag($folderid, $id, $flags, $contentParameters)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->SetReadFlag()");
        return false;
    }

    /**
     * Called when the user has requested to delete (really delete) a message
     *
     * @param string        $folderid       id of the folder
     * @param string        $id             id of the message
     *
     * @access public
     * @return boolean                      status of the operation
     * @throws StatusException              could throw specific SYNC_STATUS_* exceptions
     */
    public function DeleteMessage($folderid, $id, $contentParameters)
    {
        ZLog::Write(LOGLEVEL_INFO, "BackendRoundcubeContacts->DeleteMessage()");
        $query = sprintf("UPDATE %scontacts SET del='1',changed=NOW() where contact_id='$id' and user_id='$this->db_user_id'", ROUNDCUBE_CONTACT_BD_PREFIX);
        $result = $this->contactsdb->query($query);
        //mysql_query($query, $this->contactsdb) or print (mysql_error($this->contactsdb));
        return ($result !== false);
    }

    /**
     * Called when the user moves an item on the PDA from one folder to another
     * not implemented
     *
     * @param string        $folderid       id of the source folder
     * @param string        $id             id of the message
     * @param string        $newfolderid    id of the destination folder
     *
     * @access public
     * @return boolean                      status of the operation
     * @throws StatusException              could throw specific SYNC_MOVEITEMSSTATUS_* exceptions
     */
    public function MoveMessage($folderid, $id, $newfolderid, $contentParameters)
    {
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->MoveMessage()");
        $result = false;
        if (strcmp("$newfolderid", 'DeletedItems') === 0) {
            $del = 1;
            $query = sprintf("UPDATE rc_contacts SET del='1',changed=NOW() where contact_id='$id' and user_id='$this->db_user_id'", ROUNDCUBE_CONTACT_BD_PREFIX);
            $result = $this->contactsdb->query($query);
        }
        return ($result !== false);
    }

    /**
     * Resolves recipients
     *
     * @param SyncObject        $resolveRecipients
     *
     * @access public
     * @return SyncObject       $resolveRecipients
     */
    public function ResolveRecipients($resolveRecipients)
    {
        // TODO:
        return false;
    }

    public function GetSupportedASVersion()
    {
        return ZPush::ASV_14;
    }

    /**----------------------------------------------------------------------------------------------------------
     * private vcard-specific internals
     */

    private function dbConnectContacts()
    {
        ZLog::Write(LOGLEVEL_DEBUG, 'BackendRoundcubeContacts->dbConnectContacts()');

        $pdostr = sprintf("mysql:dbname=%s;host=%s", ROUNDCUBE_CONTACT_DB_NAME, ROUNDCUBE_CONTACT_DB_HOST);
        try {
            $this->contactsdb = new PDO($pdostr, ROUNDCUBE_CONTACT_DB_USER, ROUNDCUBE_CONTACT_DB_PASS);
        } catch (PDOException $e) {
            ZLog::Write('BackendRoundcubeContacts->dbConnectContacts() : Unable to open contacts database');
            $this->contactsdb = false;
            return $this->contactsdb;
        }

        $this->contactsdb->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        return $this->contactsdb;
    }

    private function dbConnectAuth()
    {
        ZLog::Write(LOGLEVEL_DEBUG, 'BackendRoundcubeContacts->dbConnectAuth()');

        $pdostr = sprintf("mysql:dbname=%s;host=%s", ROUNDCUBE_AUTH_DB_NAME, ROUNDCUBE_AUTH_DB_HOST);
        try {
            $this->authdb = new PDO($pdostr, ROUNDCUBE_AUTH_DB_USER, ROUNDCUBE_AUTH_DB_PASS);
        } catch (PDOException $e) {
            ZLog::Write('BackendRoundcubeContacts->dbConnectAuth() : Unable to open auth database');
            $this->authdb = false;
            return $this->authdb;
        }

        $this->authdb->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        return $this->authdb;
    }

    private function dbSelectUserId($username)
    {
        // select the user id from the database with the username that's logging in
        ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->dbSelectUserId(%s)", $username));

        $query = sprinf("SELECT user_id FROM %susers where username=?", ROUNDCUBE_CONTACT_BD_PREFIX);
        $stmt = $this->contactsdb->prepare($query);
        $stmt->execute([$username]);

        $user_id = false;

        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $user_id = $row['user_id'];
        }

        return $user_id;
    }

    private function dbCreateUser($username)
    {
        // add a new user row to theusers table for our user
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->dbCreateUser($username)");

        $query = sprintf("SELECT DISTINCT mail_host,language FROM %susers LIMIT 1", ROUNDCUBE_CONTACT_BD_PREFIX);
        $result = mysql_query($query, $this->contactsdb) or ZLog::Write(LOGLEVEL_ERROR, (mysql_error($this->contactsdb)));
        $mail_host = "";
        $lang = "";

        while ($result_rows = mysql_fetch_array($result)) {
            $mail_host = $result_rows[0];
            $lang = $result_rows[1];
        }

        $insert = sprintf("INSERT INTO %susers (username, created, mail_host, language) VALUES ('$username', NOW(), '$mail_host', '$lang')", ROUNDCUBE_CONTACT_BD_PREFIX);
        mysql_query($insert, $this->contactsdb) or ZLog::Write(LOGLEVEL_DEBUG, (mysql_error($this->contactsdb)));
    }

    private function getUserId($username)
    {
        // Get the user ID, if none is found, create one
        ZLog::Write(LOGLEVEL_DEBUG, "BackendRoundcubeContacts->getUserId($username)");

        $user_id = $this->dbSelectUserId($username);

        /*
        if (strcmp($user_id, "~none~") === 0 && ROUNDCUBE_CONTACTS_AUTO_CREATE_USER === true) {
            $this->dbCreateUser($clean_username);
            $user_id = $this->dbSelectUserId($clean_username);
        }
        */

        return $user_id;
    }

    private function getEncryptedPassword($username)
    {
        ZLog::Write(LOGLEVEL_DEBUG, sprintf("BackendRoundcubeContacts->getEncryptedPassword(): '%s'", $username));
        $password = "not found";

        $query = str_replace('%u', $username, ROUNDCUBE_PASSWORD_SQL);
        $stmt = $this->authdb->prepare($query);
        $stmt->execute();

        while ($result_rows = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            // Remove any {SPEC} password hasing spec that Dovecot might need (PHP does not need it)
            // See http://wiki.dovecot.org/Authentication/PasswordSchemes
            $password = preg_replace('/^{.*}/', '', $result_rows['crypt']);
        }

        return $password;
    }

    /**
     * Escapes a string
     *
     * @param string        $data           string to be escaped
     *
     * @access private
     * @return string
     */
    private function escape($data)
    {
        if (is_array($data)) {
            foreach ($data as $key => $val) {
                $data[$key] = $this->escape($val);
            }

            return $data;
        }

        $data = str_replace("\r\n", "\n", $data);
        $data = str_replace("\r", "\n", $data);
        $data = str_replace(array('\\', ';', ',', "\n"), array('\\\\', '\\;', '\\,', '\\n'), $data);
        return u2wi($data);
    }

    /**
     * Un-escapes a string
     *
     * @param string        $data           string to be un-escaped
     *
     * @access private
     * @return string
     */
    private function unescape($data)
    {
        $data = str_replace(array('\\\\', '\\;', '\\,', '\\n','\\N'), array('\\', ';', ',', "\n", "\n"), $data);
        return $data;
    }

}

