import importlib.util, pathlib, tempfile, unittest
path=pathlib.Path(__file__).resolve().parents[1]/'scripts/security.py'
spec=importlib.util.spec_from_file_location('security',path);security=importlib.util.module_from_spec(spec);spec.loader.exec_module(security)

class FormatGuard(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.tmp.name);self.addCleanup(self.tmp.cleanup)
  (self.root/'.approved-new-cluster').write_text('new-only');self.c=self.root/'server.properties'
  self.base='authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer\nallow.everyone.if.no.acl.found=false\nsuper.users=User:bootstrap\n'
 def config(self,path):self.c.write_text(self.base+'log.dirs='+str(path)+'\n')
 def test_empty_subdirectory_allowed(self):self.config(self.root/'data');security.check_new(self.root,self.c)
 def test_existing_metadata_refused(self):
  p=self.root/'data';p.mkdir();(p/'meta.properties').write_text('existing');self.config(p)
  with self.assertRaises(ValueError):security.check_new(self.root,self.c)
 def test_outside_root_refused(self):
  self.config(self.root.parent/'external-data')
  with self.assertRaises(ValueError):security.check_new(self.root,self.c)
 def test_symlink_to_existing_refused(self):
  p=self.root/'original';p.mkdir();(p/'meta.properties').write_text('existing');(self.root/'link').symlink_to(p);self.config(self.root/'link')
  with self.assertRaises(ValueError):security.check_new(self.root,self.c)
 def test_world_readable_secret_refused(self):
  self.c.write_text('placeholder');self.c.chmod(0o644)
  with self.assertRaises(ValueError):security.private_file(self.c)
 def test_private_secret_allowed(self):
  self.c.write_text('placeholder');self.c.chmod(0o600);security.private_file(self.c)
 def test_secret_symlink_refused(self):
  self.c.write_text('placeholder');self.c.chmod(0o600);p=self.root/'secret-link';p.symlink_to(self.c)
  with self.assertRaises(ValueError):security.private_file(p)
 def test_anonymous_super_refused(self):
  self.base+='super.users=User:ANONYMOUS\n';self.config(self.root/'data')
  with self.assertRaises(ValueError):security.check_new(self.root,self.c)

if __name__=='__main__':unittest.main()
