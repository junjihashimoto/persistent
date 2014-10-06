{-# LANGUAGE OverloadedStrings #-}
module Database.Persist.Zookeeper.Internal
       where
  
import Control.Monad.IO.Class (MonadIO (..))
import Data.Monoid
import Data.Maybe
import qualified Data.Aeson as A
import qualified Data.Text as T
import Database.Persist.Types
import Database.Persist.Class
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Base64.URL as B64
import qualified Data.Map as M


txtToKey :: (PersistEntity val) => T.Text -> Key val
txtToKey txt = 
  case (keyFromValues [PersistText txt]) of
    Right v -> v
    Left v -> error $ T.unpack v

keyToTxt :: (PersistEntity val) => Key val -> T.Text
--keyToTxt (Key (PersistText key)) = key
keyToTxt key = 
  case keyToValues key of
    [PersistText txt] -> txt
    _ -> error "keyToTxt"
--keyToTxt v = error $ "do not support "++show v

dummyFromKey :: Key v -> Maybe v
dummyFromKey _ = Nothing

dummyFromFList :: [Filter v] -> v
dummyFromFList _ = error "huga"

dummyFromUnique :: Unique v -> Maybe v
dummyFromUnique _ = Nothing

val2table :: (PersistEntity val) => val -> T.Text
val2table = unDBName . entityDB . entityDef . Just

val2uniqkey :: (MonadIO m, PersistEntity val) => val -> m (Maybe (Unique val))
val2uniqkey val = do
  case persistUniqueKeys val of
    (uniqkey:_) -> return $ Just uniqkey
    [] -> return Nothing

uniqkey2key :: (PersistEntity val) => Unique val -> Key val
uniqkey2key uniqkey =
  let dir = entity2path $ fromJust $ dummyFromUnique uniqkey
    in txtToKey $ T.pack $ dir <> "/" <>  (B.unpack $ B64.encode $ BL.toStrict $ A.encode $ persistUniqueToValues uniqkey)

entity2bin :: (PersistEntity val) => val -> B.ByteString
entity2bin val = BL.toStrict (A.encode (map toPersistValue (toPersistFields val)))

kv2v :: [PersistValue] -> [PersistValue]
kv2v [] = []
kv2v ((PersistList [_k,v] ):xs) = v:kv2v xs
kv2v (x:xs) = x:kv2v xs

bin2entity :: (PersistEntity val) => B.ByteString -> Maybe val
bin2entity bin =
  case A.decode (BL.fromStrict bin) :: Maybe [PersistValue]of
    Nothing -> Nothing
    Just v ->
      case fromPersistValues (kv2v v) of
        Right body  -> Just $ body
        Left s -> error $ T.unpack s
  

entity2path :: (PersistEntity val) => val -> String
entity2path val = "/" <> (T.unpack $ val2table val)

-- entityAndKey2path :: (PersistEntity val) => val -> Key val -> String
-- entityAndKey2path val (Key (PersistText txt)) = entity2path val <> "/" <>  ( B.unpack $ B64.encode $ B.pack $ T.unpack txt)
-- entityAndKey2path _ _ = error "key is not persist text"

-- key2path :: (PersistEntity val) => Key val -> String
-- key2path key = entityAndKey2path (fromJust (dummyFromKey key)) key



filter2path :: (PersistEntity val) => [Filter val] -> String 
filter2path filterList = entity2path $ dummyFromFList filterList

getMap :: PersistEntity val => val -> M.Map T.Text PersistValue
getMap val =  M.fromList $ getList val
getList :: PersistEntity val => val -> [(T.Text,PersistValue)]
getList val =  
  let fields = fmap toPersistValue (toPersistFields val)
      in zip (getFieldsName val) fields
getFieldsName :: (PersistEntity val) => val -> [T.Text]
getFieldsName val =  fmap (unDBName.fieldDB) $ entityFields $ entityDef $ Just val
getFieldName :: (PersistEntity val,PersistField typ) => EntityField val typ -> T.Text
getFieldName field =  unDBName $ fieldDB $ persistFieldDef $ field
fieldval :: (PersistEntity val,PersistField typ) => EntityField val typ -> val -> PersistValue
fieldval field val =  (getMap val) M.! (getFieldName field)

updateEntity :: PersistEntity val =>  val -> [Update val] -> Either T.Text val
updateEntity val upds = 
  fromPersistValues $ map snd $ foldl updateVals (getList val) upds


updateVals :: PersistEntity val =>  [(T.Text,PersistValue)] -> Update val -> [(T.Text,PersistValue)]
updateVals [] _ = []
updateVals ((k,v):xs) u@(Update field _ _) = 
  if getFieldName field == k
    then (k,updateVal v u):xs
    else (k,v):updateVals xs u
updateVals _ _ = error "not supported"

updateVal :: PersistEntity val =>  PersistValue -> Update val -> PersistValue
updateVal _v (Update _ val upd) = 
  case upd of
    Assign -> toPersistValue val
    _ -> error "not support"
    -- Add -> (+) <$> v <$> toPersistValue val 
    -- Subtract -> v - toPersistValue val 
    -- Multiply -> v * toPersistValue val 
    -- Divide -> v `div` toPersistValue val 
updateVal _v _ = error "not supported"

