{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Query
       where

import Database.Persist
import Data.Monoid
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Reader
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store ()
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Acquire

instance PersistQuery Z.Zookeeper where
  updateWhere filterList valList = do
    stat <- ask
    srcRes <- selectKeysRes filterList []
    liftIO $ with srcRes ( $$ loop stat)
    where
      loop stat = do
        key <- await
        case key of
          Just key' -> do
            liftIO $ flip runReaderT stat $ update key' valList
            loop stat
          Nothing ->
            return ()
  deleteWhere filterList = do
    (str::[String]) <- execZookeeper $ \zk -> do
      Z.getChildren zk (filter2path filterList) Nothing
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = txtToKey $ T.pack $ (filter2path filterList) <> "/" <> x
        va <- get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then delete key
              else return ()
        loop xs
  selectSourceRes filterList [] = do
    stat <- ask
    (str::[String]) <- liftIO $ flip runReaderT stat $ execZookeeper $ \zk -> do
      Z.getChildren zk (filter2path filterList) Nothing
    return $ return $ loop stat str
    where
      loop _ [] = return ()
      loop stat (x:xs) = do
        let key = txtToKey $ T.pack $ (filter2path filterList) <> "/" <> x
        va <- liftIO $ flip runReaderT stat $ get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then yield $ Entity key v
              else return ()
        loop stat xs
  selectSourceRes _ _ = error "not supported selectOpt"
  selectFirst filterList [] =  do
    srcRes <- selectSourceRes filterList []
    liftIO $ with srcRes ( $$ CL.head)
  selectFirst _ _ = error "not supported selectOpt"
  selectKeysRes filterList [] = do 
    stat <- ask
    (str::[String]) <- liftIO $ flip runReaderT stat $ execZookeeper $ \zk -> 
      Z.getChildren zk (filter2path filterList) Nothing
    return $ return (loop stat str)
    where
      loop _ [] = return ()
      loop stat (x:xs) = do
        let key = txtToKey $ T.pack $ (filter2path filterList) <> "/" <> x
        va <- liftIO $ flip runReaderT stat $ get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then yield key
              else return ()
        loop stat xs
  selectKeysRes  _ _ = error "not supported selectOpt"
  count filterList = do
    v <- selectList filterList []
    return $ length v

  
dummyFromFilts :: [Filter v] -> Maybe v
dummyFromFilts _ = Nothing

data OrNull = OrNullYes | OrNullNo


filterClauseHelper :: PersistEntity val
             => Bool -- ^ include WHERE?
             -> OrNull
             -> val
             -> [Filter val]
             -> (Bool, T.Text, [PersistValue])
filterClauseHelper includeWhere orNull val filters =
    (bool, if not (T.null sql) && includeWhere
            then " WHERE " <> sql
            else sql, vals)
  where
    (bool, sql, vals) = combineAND filters
    combineAND = combine " AND " (&&)
    combineOR = combine " OR " (||)

    combine s op fs =
        (foldr1 op c ,T.intercalate s $ map wrapP a, mconcat b)
      where
        (c, a, b) = unzip3 $ map go fs
        wrapP x = T.concat ["(", x, ")"]
    go (BackendFilter _) = error "BackendFilter not expected"
    go (FilterAnd []) = (True,"1=1", [])
    go (FilterAnd fs) = combineAND fs
    go (FilterOr []) = (True,"1=0", [])
    go (FilterOr fs)  = combineOR fs
    go (Filter field value pfilter) = 
      (showSqlFilter' pfilter (fieldval field val) allVals, 
      name <> ":"
      <> T.pack (show (fieldval field val)) <> ":" 
      <> showSqlFilter pfilter
      <> T.pack (show (showSqlFilter' pfilter (fieldval field val) allVals))
      <> "?5:" <> T.pack (show allVals) <> orNullSuffix, allVals) 
      where
        
        filterValueToPersistValues :: forall a.  PersistField a => Either a [a] -> [PersistValue]
        filterValueToPersistValues v = map toPersistValue $ either return id v

        orNullSuffix =
            case orNull of
                OrNullYes -> mconcat [" OR ", name, " IS NULL"]
                OrNullNo -> ""

        allVals = filterValueToPersistValues value
        name = unDBName $ fieldDB $ persistFieldDef field
        showSqlFilter Eq = "="
        showSqlFilter Ne = "<>"
        showSqlFilter Gt = ">"
        showSqlFilter Lt = "<"
        showSqlFilter Ge = ">="
        showSqlFilter Le = "<="
        showSqlFilter In = " IN "
        showSqlFilter NotIn = " NOT IN "
        showSqlFilter (BackendSpecificFilter s) = s
        showSqlFilter' :: PersistFilter -> PersistValue -> [PersistValue] -> Bool
        showSqlFilter' Eq a b = (==) a (head b)
        showSqlFilter' Ne a b = (/=) a (head b)
        showSqlFilter' Gt a b = (>)  a (head b)
        showSqlFilter' Lt a b = (<)  a (head b)
        showSqlFilter' Ge a b = (>=) a (head b)
        showSqlFilter' Le a b = (<=) a (head b)
        showSqlFilter' In _ [] = False
        showSqlFilter' In a (x:xs) = if a==x then True else showSqlFilter' In a xs
        showSqlFilter' NotIn _ [] = True
        showSqlFilter' NotIn a (x:xs) = if a==x then False else showSqlFilter' NotIn a xs
        showSqlFilter' (BackendSpecificFilter _s) _ _ =  error "not supported"


filterClause :: PersistEntity val
             => val
             -> [Filter val]
             -> (Bool, T.Text, [PersistValue])
filterClause val = filterClauseHelper True OrNullNo val
