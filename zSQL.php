<?php
// version: 1
namespace za\zSQL;

// ザガタ。六 /////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////

class zSQL {
	/* Zagata.MySQL */
	private $sort = 'asc';
	private $limt = false;

	public function load() {
		// $this->za->msg('ntf','db', 'love.xml connected');

		// define list of fields of main table
		$this->r = array();
		$qu = 'describe '.$this->prfx.'_r';
		$re = $this->x->query($qu);
		if($re) { 
			$re = $re->fetchAll(\PDO::FETCH_ASSOC);
			foreach($re as $v) { $this->r[] = current($v); }
			// fields of big table
			$this->b = array('de','lid','imgti');

			$this->za->ee($this->n.'_ready');
		} else {
			$err = print_r($this->x->errorInfo(),true);
			$this->za->msg('err',$this->n,$qu."\n".$err);
		}
	}
	
	private function save() {
		// $this->x->commit();
	return false;
	}
	
	/////////////////////////////// 
	// funcs
	public function max($k,$idp=false) {
		$r = in_array($k,$this->r);
		$sql = 'select max('.(($r)?$k:'v').') from '.$this->prfx.'_'.((in_array($k,$this->r))?'r':'s');
		$sql.= (!$r)?' where k="'.$k.'"':'';
		if(is_numeric($idp)) {
			$sql.= ((!$r)?' and':' where').' idp="'.$idp.'"';
		} elseif(is_string($idp)) {
			$sql.= ((!$r)?' and':' where').' ca="'.$idp.'"';
		} else {}
		
		   
		$tmp = $this->x->query($sql,\PDO::FETCH_ASSOC);
		foreach($tmp as $v) { return (int) current($v); }
	return false;
	}

	public function srt($s=false) {
		$this->sort = ($s)?$s:'asc';
	return $this;
	}
	public function lim($s=false) {
		$this->limt = ($s)?$s:false;
	return $this;
	}
	
	public function q($qu,$add=true) {
		$re = false;
		$re = $this->x->query($qu);
		if($re) {
			$re = $re->fetchall(\PDO::FETCH_ASSOC);
			// get things
			if($add) {
				$ids = array(); $ix = count($re);
				if($ix>0) {
					for($i=0;$i<$ix;$i++) { $ids[] = $re[$i]['id']; }
					$ids = implode(',',$ids);
					$qu = 'select idp,k,v from '.$this->prfx.'_s where idp in ('.$ids.') union select idp,k,v from '.$this->prfx.'_b where idp in ('.$ids.')';
					$tmp = $this->x->query($qu);
					$tmp = $tmp->fetchall(\PDO::FETCH_ASSOC);
					$iy = count($tmp);
					if($iy>0) {
						for($y=0;$y<$iy;$y++) {
							for($i=0;$i<$ix;$i++) { if($re[$i]['id'] == $tmp[$y]['idp']) {
								$re[$i][ $tmp[$y]['k'] ] = $tmp[$y]['v'];
							} else {} }
						}
					} else { }
				} else { } 
			} else { } 
		} else {
			$err = print_r($this->x->errorInfo(),true);
			$this->za->msg('err',$this->n,$qu."\n".$err);
		}		
	return ((is_array($re)&&count($re)>0)||!is_array($re))?$re:false;
	}
	
	public function updwn($id,$d='upp') {
		// !!!
		$tmp = $this->qq($id)[0];
		$tmp['z'] = (isset($tmp['z']))?$tmp['z']:0; // have to send return false
		
		$qu = 'select id,z from '.$this->prfx.'_r where (idx is NULL or idx!="*") and idp="'.$tmp['idp'].'" and z '.((($d=='upp'&&$this->sort=='asc')||($d=='dwn'&&$this->sort=='desc'))?'<':'>').' '.$tmp['z'].' order by z '.$this->sort.' limit 1';
		$re = $this->x->query($qu);
		if($re) {
			$re = $re->fetchall(\PDO::FETCH_ASSOC);
			$re = $re[0];
			$z = $re['z']; $re['z'] = $tmp['z']; $tmp['z'] = $z;

			$tmp = $this->qq($tmp['id'],array('id'=>$tmp['id'],'lbd'=>time(),'z'=>$tmp['z']));
			if($tmp) {
				$tmp = $this->qq($re['id'],array('id'=>$re['id'],'lbd'=>time(),'z'=>$re['z']));
			} else { }
			return $tmp;
		} else {
			$err = print_r($this->x->errorInfo(),true);
			$this->za->msg('err',$this->n,'UpDwn: no place to go '.$err); 
		}
	return false; 
	}
	
	public function qq($k,$v=false) {
		if($k == 'del' && $v) { // del
			try {
				$del = array();
				
				if(is_array($v)) {
					$qu = array();
					if(isset($v['id'])) { $qu[] = ' id="'.$v['id'].'"'; } else {}
					if(isset($v['ids'])) { $qu[] = 'ids like "'.$v['ids'].$v['id'].','.'%"'; } else {}
					$qu = implode(" or ",$qu);
				} elseif(strpos($v,',')!==false) {
					$qu = 'ids like "'.$v.'%"';
				} else {
					$qu = 'id = "'.$v.'" ';
				}

				// second _i deleting - first
				$tmp = $this->x->query('delete from '.$this->prfx.'_b where idp in (select id from '.$this->prfx.'_r where '.trim($qu).')',\PDO::FETCH_ASSOC);
				if($tmp!==false) {
					$tmp = $this->x->query('delete from '.$this->prfx.'_s where idp in (select id from '.$this->prfx.'_r where '.trim($qu).')',\PDO::FETCH_ASSOC);
					if($tmp!==false) {
						$tmp = $this->x->query('delete from '.$this->prfx.'_r where '.trim($qu),\PDO::FETCH_ASSOC);
						if($tmp==false) {
							$this->za->msg('err',$this->n,'error deleting from _r');
						} else { return true; }
					} else {
						$this->za->msg('err',$this->n,'error deleting from _s');
					}
				} else {
					$this->za->msg('err',$this->n,'error deleting from _b');
				}
				// exit($this->x->asXML());
				// $this->save();
			} catch (Exception $e) {
				$this->za->msg('err',$this->n,$e->getMessage());
			}
			return false;
		} elseif($k && $v) { // red
			try {
				// $this->za->hdr();
				$tmp = true;
				$tmp_i = array();
				$ttmp = $this->x->query('select distinct k from '.$this->prfx.'_s where idp='.$v['id'].' union select distinct k from '.$this->prfx.'_b where idp='.$v['id'],\PDO::FETCH_ASSOC);
				foreach($ttmp as $tv) { $tmp_i[] = current($tv); } unset($ttmp,$tv);

				foreach($v as $kk=>$vv) {
					if(in_array($kk,array('dpd','dlbd','dfd','xpd','xlbd','xfd'))) {
						unset($v[$kk]);
					} elseif(!in_array($kk,$this->r)) {
						if(in_array($kk,$tmp_i)) {
							$qu = 'update '.$this->prfx.'_'.((in_array($kk,$this->b)||is_array($vv))?'b':'s').' set v=:v where idp=:idp and k=:k ';
						} else {
							$qu = 'insert into '.$this->prfx.'_'.((in_array($kk,$this->b)||is_array($vv))?'b':'s').' (idp,k,v) values (:idp,:k,:v)';
						}
						$tmp = $this->x->prepare($qu);
						// print($qu."\n");
						// $tmp = $this->x->query($qu);
						if(is_array($vv)) { $vv = json_encode($vv); } else { }
						// $this->za->msg('ntf',$this->n, $k.' '.$kk.' '.$vv);
						$tmp = $tmp->execute(array(':idp'=>$k,':k'=>$kk,':v'=>$vv));
						unset($v[$kk]);
						if($tmp===false) {
							$this->za->msg('err',$this->n,'error updating _'.((in_array($kk,$this->b||is_array($vv)))?'b':'s'));
							break;
						} else {}
					} else {}
				}
				if($tmp!=false) {
					$qu = array();
					foreach($v as $kk=>$vv) { 
						if($kk=='idp') {
							$qu[] = $kk.'='.(($vv)?$vv:'null');
						} else {
							$qu[] = $kk.((in_array($kk,array('pd','lbd','fd')))?(($vv)?'=from_unixtime('.$vv.')':'=null'):'="'.$vv.'"'); 
						}
					}
					$qu = 'update '.$this->prfx.'_r set '.implode(', ',$qu).' where id='.$k;
					$tmp = $this->x->query($qu);
					if($tmp===false) {
						$this->za->msg('err',$this->n,'error updating _r '.$qu);
					} else { return true; }
					// exit($this->x->asXML());
					// $this->save();
				} else {}
				// exit($this->x->asXML());
				// $this->save();
			} catch (Exception $e) {
				$this->za->msg('err',$this->n,'red ['.$qu.'] '.$e->getMessage());
			}
			return false;
		} elseif($k!==false) { // get
			$re = array();
			$qu = false;

			if(strpos($k,',')!==false) {
				$qu = ' (idx is null or idx="") and ids like "'.$k.'%" ';
			} elseif($k == '*') {
				$qu = ' idx="*" ';
			} elseif(is_numeric($k)) {
				$qu = ' id='.$k.' '; // and (not(./idx) or ./idx!="*")
			} elseif(is_string($k)) {
				$qu = ' ca="'.$k.'" or li="'.$k.'" ';
			} else { // elseif(is_array($k)) {
				return false;
			}
			if(!isset($this->za->m['adm'])) {
				$qu = '('.$qu.') and (pd is null or pd <= from_unixtime('.time().')) and (fd is null or fd > from_unixtime('.time().'))';
			} else { }
			// $this->za->msg('ntf',$this->n,$qu);

			try {
				$re = $this->x->query('select *, unix_timestamp(pd) as pd, unix_timestamp(lbd) as lbd, unix_timestamp(fd) as fd from '.$this->prfx.'_r where '.trim($qu));
				$re = $re->fetchall(\PDO::FETCH_ASSOC);
				$ix = count($re); 
				if($ix>0) {
					$qu = array();
					for($i=0;$i<$ix;$i++) {
						$qu[] = $re[$i]['id'];
						foreach(array('pd','lbd','fd') as $v) {
							if(isset($re[$i][$v])) { 
								$re[$i]['d'.$v] = date('d.m.Y H:i:s T',$re[$i][$v]); 
								$re[$i]['x'.$v] = date(DATE_W3C,$re[$i][$v]); 
							} else {}
						}
					}
					$qu = 'select * from '.$this->prfx.'_s where idp '.((count($qu)==1)?'= '.$qu[0]:'in ('.implode(',',$qu).') ').' union select * from '.$this->prfx.'_b where idp '.((count($qu)==1)?'= '.$qu[0]:'in ('.implode(',',$qu).')');
					$tre = $this->x->query($qu);
					$tre = $tre->fetchall(\PDO::FETCH_ASSOC);
					$iy = count($tre);
					for($i=0;$i<$ix;$i++) {
						for($y=0;$y<$iy;$y++) {
							if($re[$i]['id'] == $tre[$y]['idp']) {
								$vv = $tre[$y]['v'];
								if(strpos($vv,'[')===0||strpos($vv,'{')===0) { 
									$vv = json_decode($vv,true);
								} else {}
								$re[$i][$tre[$y]['k']] = $vv;
							} else {}
						}
					}
				}
			} catch (\PDOException $e) {
				$this->za->msg('err',$this->n,$qu."\n".$e->getMessage());
			}
			return ((is_array($re)&&count($re)>0)||!is_array($re))?$re:false;
		} elseif($v) { // add
			try {
				// ???
				foreach($v as $kk=>$vv) {
					$vv_arr = is_array($vv);
					$vv = (is_array($vv))?addslashes(json_encode($vv)):addslashes($vv);
					if(in_array($kk,array('dpd','dlbd','dfd','xpd','xlbd','xfd'))) {
						unset($v[$kk]);
					} elseif(!in_array($kk,$this->r)) {
						$sql = 'insert into '.$this->prfx.'_'.((in_array($kk,$this->b)||$vv_arr)?'b':'s').' (idp,k,v) values ('.$v['id'].',"'.$kk.'", "'.$vv.'")';
						$tmp = $this->x->query($sql);
						unset($v[$kk]);
						if($tmp===false) {
							$err = print_r($this->x->errorInfo(),true);
							$this->za->msg('err',$this->n,'error inserting to _'.((in_array($kk,$this->b)||$vv_arr)?'b':'s').' '.$err);
							break;
						} else {}
					} else {}
				}
				if($tmp!=false) {
					$kx = array(); $vx = array();
					foreach($v as $kk=>$vv) {
						$kx[] = $kk;
						$vx[] = ((in_array($kk,array('pd','lbd','fd')))?'from_unixtime('.$vv.')':'"'.$vv.'"');
					}
					$sql = 'insert into '.$this->prfx.'_r ('.implode(',',$kx).') values ('.implode(',',$vx).')';
					$tmp = $this->x->query($sql);
					
					if($tmp===false) {
						$err = print_r($this->x->errorInfo(),true);
						$this->za->msg('err',$this->n,'error updating _r '.$err );
					} else { return true; }
					// exit($this->x->asXML());
					// $this->save();
				} else {
					$this->za->msg('err',$this->n,'error inserting to _r');
				}
			} catch (Exception $e) {
				$this->za->dbg('adding: '.$e->getMessage().' '.$sql);
				$this->za->msg('err',$this->n,$e->getMessage());
			}
			return false;
		} else {
			$this->za->msg('err','zSQL','no condition accepted by qq: '.$k);
		}
	return $this;
	}
	
	public function pp($k,$pg=10,$p=1) {
		$re = array(); $tmp = array();

		// elements of page
		$pg = (integer) $pg; $p = (integer) $p;
		
		$re = $this->x->query('select count(id) as coid from '.$this->prfx.'_r where ids="'.$k.'" and (pd is null or pd <= from_unixtime('.time().')) and (fd is null or fd > from_unixtime('.time().')) ');
		$re = $re->fetchAll(\PDO::FETCH_ASSOC);
		$ix = $re[0]['coid'];
		$this->za->mm('vrs',array('pages'=>ceil($ix / $pg)));
		
		$qu = 'select *, unix_timestamp(pd) as pd, unix_timestamp(lbd) as lbd, unix_timestamp(fd) as fd from '.$this->prfx.'_r where ids="'.$k.'" and (pd is null or pd <= from_unixtime('.time().')) and (fd is null or fd > from_unixtime('.time().')) order by z '.$this->sort.' limit '.($pg*($p-1)).', '.$pg;
		// $this->za->msg('ntf',$this->n,$qu);
		$re = $this->x->query($qu);
		$re = $re->fetchAll(\PDO::FETCH_ASSOC);
		$ix = count($re); $qu = array();
		for($i=0;$i<$ix;$i++) {
			// additional info
			$qu[] = $re[$i]['id'];
			
			foreach(array('pd','lbd','fd') as $v) {
				if(isset($re[$i][$v])) { 
					$re[$i]['d'.$v] = date('d.m.Y H:i:s T',$re[$i][$v]); 
					$re[$i]['x'.$v] = date(DATE_W3C,$re[$i][$v]); 
				} else {}
			}
			
			// child elements
			$tmp = ((isset($re[$i]['ids']))?$re[$i]['ids']:'').$re[$i]['id'].',';
			$tmp = $this->qq( $tmp ); // <!-- ЗДЕСЬ ОТВЕТ! ТЫ ЧЁТ НАПОРТАЧИЛ 
			if($tmp) {
				foreach($tmp as $y) {
					$re[] = $y;
				}
			} else {}
		}
		
		// additional info
		if($qu) {
			$this->za->hdr();
			$qu = 'select * from '.$this->prfx.'_s where idp '.((count($qu)==1)?'= '.$qu[0]:'in ('.implode(',',$qu).') ').' union select * from '.$this->prfx.'_b where idp '.((count($qu)==1)?'= '.$qu[0]:'in ('.implode(',',$qu).')');
			$tre = $this->x->query($qu);
			$tre = $tre->fetchall(\PDO::FETCH_ASSOC);
			$iy = count($tre);
			for($i=0;$i<$ix;$i++) {
				for($y=0;$y<$iy;$y++) {
					if($re[$i]['id'] == $tre[$y]['idp']) {
						$vv = $tre[$y]['v'];
						if(strpos($vv,'[')===0||strpos($vv,'{')===0) { 
							$vv = json_decode($vv,true);
						} else {}
						$re[$i][$tre[$y]['k']] = $vv;
					} else {}
				}
			}
		} else {}

	return ((is_array($re)&&count($re)>0)||!is_array($re))?$re:false;
	}

	public function i2a($i) {
		// result fetch
		$re = array();
		
	return $re;
	}

	private $a2q_ij = array();
	public function a2q($a,$l=0) {
		$q = '';
		if($a[0]=='not') {
			$q = "not( ".$this->a2q($a[1],1).")";
		} elseif($a[1]=='in') {
			if(in_array($a[0],$this->r)) { 
				$rs = 'r'; 
			} else { 
				$rs = (in_array($a[0],array('lid','de')))?'b':'s';
				$this->a2q_ij[] = 'inner join '.$this->prfx.'_s as '.$rs.$i.' on r.id = '.$rs.$i.'.idp and '.$rs.$i.'.k = "'.$a[0].'" '; 
			}
			$q = 'concat(",",'.$rs.'.'.$a[0].',",") like ",'.$a[2].',"';
		} elseif($a[1]=='is'&&$a[2]=='null') {
			// special construction
			$i = count($this->a2q_ij);
			$this->a2q_ij[] = 'left join '.$this->prfx.'_s as s'.$i.' on r.id = s'.$i.'.idp and s'.$i.'.k = "'.$a[0].'" ';
			$q = 's'.$i.'.v '.$a[1].' '.$a[2];
		} elseif($a[1]=='like') {
			if(is_array($a[0])||in_array($a[0],$this->r)) {
				$q = "(".((is_array($a[0]))?$this->a2q($a[0],1):'r.'.$a[0]).' like \''.$a[2].'\')';
			} else {
				$i = count($this->a2q_ij);
				$this->a2q_ij[] = 'left join '.$this->prfx.'_s as s'.$i.' on r.id = s'.$i.'.idp and s'.$i.'.k = "'.$a[0].'" ';
				$q = 's'.$i.'.v like "'.$a[2].'" ';
			}
		} else {
			$a[1] = str_replace(array('&&','&','||','|'),array('and','and','or', 'or'),trim($a[1]));
			if(is_array($a[0])||in_array($a[0],$this->r)) {
				if(in_array($a[0],array('pd','lbd','fd'))) { $a[2] = 'from_unixtime(\''.$a[2].'\')'; } elseif(!is_array($a[2])) { $a[2] = "'".$a[2]."'"; } else { }
				$q = "(".((is_array($a[0]))?$this->a2q($a[0],1):'r.'.$a[0])." ".$a[1]." ".((is_array($a[2]))?$this->a2q($a[2],1):$a[2]).")";
			} else {
				$i = count($this->a2q_ij);
				$rs = (in_array($a[0],array('lid','de')))?'b':'s';
				$this->a2q_ij[] = 'inner join '.$this->prfx.'_'.$rs.' as '.$rs.$i.' on r.id = '.$rs.$i.'.idp and '.$rs.$i.'.k = "'.$a[0].'" ';
				$q = $rs.$i.'.v '.$a[1].' "'.$a[2].'"'; // looks it is kinda work
			}
		}
		if($l==0) { // recursion level
			$ij = '';
			foreach($this->a2q_ij as $v) { $ij.= $v; } 
			$q = 'select r.*, unix_timestamp(r.pd) as pd, unix_timestamp(r.lbd) as lbd, unix_timestamp(r.fd) as fd from '.$this->prfx.'_r as r '.$ij.' where '.$q; 
			if($this->sort) { $q.= ' order by r.z '.$this->sort; } else { }
			if($this->limt) {				
				// elements of page
				// ->pp - have to change it as one solution
				$p = (integer) $this->za->mm(array('vrs','page')); 
				$p = ($p==0)?1:$p;
				
				$q.= ' limit '.(($p-1)*$this->limt).','.$this->limt;
			} else { }
			// default values
			$this->a2q_ij = array(); $this->sort = 'asc'; $this->limt = false;
		} else {}
	return $q;
	}

	/////////////////////////////// 
	// ini
	function __construct($za,$a=false,$n=false) {
		$this->za = $za;
		$this->n = (($n)?$n:'zSQL');
		// $this->za->msg('dbg','zSQL','i am '.$this->n.'(zSQL)');

		// parsing $a
		$a = explode('/',str_replace(array('://',':','@'),'/',$a));
		$this->prfx = $a[6];
		// driver, login, password, host, port, db, prefix

		try {
			$this->x = new \PDO($a[0].':host='.$a[3].(($a[4]>0)?':'.$a[4]:'').';dbname='.$a[5], $a[1], $a[2],array( \PDO::ATTR_ERRMODE => \PDO::ERRMODE_SILENT, \PDO::MYSQL_ATTR_INIT_COMMAND => 'set names utf8' ));
			// $this->x->exec('set names utf8');
			// $this->x->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_SILENT);
			// $this->za->msg('ntf','db', 'start initiate db');
			$this->za->ee($this->n,array($this,'load'));
		} catch(\PDOException $e) {
			$this->za->mm(array('vrs','e404'),true);
			$this->za->msg('err', $this->n, 'connection error '.$a[3]);
			$this->za->ee($this->n.'_ready');
		}
	}
}

////////////////////////////////////////////////////////////////

if(class_exists('\zlo')) {
	\zlo::da('zSQL');
} elseif(realpath(__FILE__) == realpath($_SERVER['DOCUMENT_ROOT'].$_SERVER['SCRIPT_NAME'])) {
	header("content-type: text/plain;charset=utf-8");
	exit('zSQL');
} else {}

?>