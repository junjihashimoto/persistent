{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Store (
  deleteRecursive
, BackendKey(..)
, txtToKey
, keyToTxt
, uniqkey2key
)where

import Database.Persist
import qualified Database.Persist.Sql as Sql
import Control.Exception
import Control.Applicative
import qualified Database.Zookeeper as Z
import Data.Monoid
import Data.Maybe
import qualified Data.Text as T
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.ZooUtil
import Control.Monad
import Control.Monad.Reader
import qualified Data.Aeson as A

import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Base64.URL as B64


deleteRecursive :: (Monad m, MonadIO m) => String -> Action m ()
deleteRecursive dir = execZookeeper $ \zk -> zDeleteRecursive zk dir

instance Sql.PersistFieldSql (BackendKey Z.Zookeeper) where
    sqlType _ = Sql.SqlOther "doesn't make much sense for Zookeeper"

instance A.ToJSON (BackendKey Z.Zookeeper) where
    toJSON (ZooKey key) = A.String key

instance A.FromJSON (BackendKey Z.Zookeeper) where
    parseJSON (A.String key) = pure $ ZooKey key
    parseJSON _ = mzero

instance PersistStore Z.Zookeeper where
    newtype BackendKey Z.Zookeeper = ZooKey { unZooKey :: T.Text }
        deriving (Show, Read, Eq, Ord, PersistField)

    insert val = do
      let dir = entity2path val
      str <- execZookeeper $ \zk -> do
        -- let bin = entity2bin val
        -- let ent = (bin2entity bin::)
        -- print $ show $ entity2bin val
--        print $ show $ bin'
--        print $ show $ (A.decode (BL.fromStrict (fromJust bin)) :: Maybe [PersistValue])
        zCreate zk dir "" (Just (entity2bin val)) [Z.Sequence]
      return $ txtToKey str

    insertKey key val = do
      -- liftIO $ print ("call insertkey"::String)
      -- liftIO $ print $ show key
      -- liftIO $ print $ show val
      -- liftIO $ print $ show $ keyToTxt key
      -- liftIO $ print $ show $ entity2bin val
      _ <- execZookeeper $ \zk -> do
        let dir = entity2path val
        zCreate zk dir (keyToTxt key) (Just (entity2bin val)) []
      return ()

    repsert key val = do
      _ <- execZookeeper $ \zk -> do
        let dir = entity2path val
        zRepSert zk dir (keyToTxt key) (Just (entity2bin val))
      return ()

    replace key val = do
      execZookeeper $ \zk -> do
        let dir = entity2path val
        _ <- zReplace zk dir (keyToTxt key) (Just (entity2bin val))
        return $ Right ()
      return ()

    delete key = do
      execZookeeper $ \zk -> do
        let dir = key2path key
        _ <- zDelete zk dir (keyToTxt key) Nothing
        -- print $ "del:" ++ show (keyToTxt key)
        return $ Right ()
      return ()

    get key = do
      r <- execZookeeper $ \zk -> do
        let dir = key2path key
        val <- zGet zk dir (keyToTxt key)
        return $ Right val
      case r of
        (Left Z.NoNodeError) ->
          return Nothing
        (Left v) ->
          fail $ show v
        (Right (Just str,_sta)) -> do
          let ent = (bin2entity str)
          -- let dec = A.decode (BL.fromStrict str) :: Maybe [PersistValue] 
          -- liftIO $ print $ show dec
          -- case dec of
          --   Nothing -> return Nothing
          --   Just v ->
          --     case fromPersistValues v of -- (kv2v v) of
          --       Right body  -> return $ Just $ body
          --       Left s -> error $ T.unpack s
--          print $ show ent
          return ent
        (Right (Nothing,_stat)) -> do
          fail $ "data is nothing"
      
    update key valList = do
      va <- get key
      case va of
        Nothing -> return ()
        Just v ->
          case updateEntity v valList of
            Right v' -> 
              replace key v'
            Left v' -> error $ show v'
